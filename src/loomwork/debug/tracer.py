"""Tick-by-tick replay tracing.

Mirrors the replay algorithm of ADR-0002 (also implemented, for real replay, by
:class:`loomwork.runtime.replayer.Replayer`): feed each tick's inputs to the workflow
code, then compare the commands it produces against the decision events recorded after
it. The difference here is what happens on a mismatch: :class:`Replayer` raises
immediately, because a running worker cannot do anything else with a broken history.
:func:`trace_replay` instead keeps going until it must stop, recording exactly where and
why replay stopped matching (a :class:`Divergence`), so a debugger can show everything
that replayed correctly *and* explain the first thing that did not.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loomwork.codec.converter import DataConverter, default_converter
from loomwork.debug.errors import DebugError
from loomwork.errors import NotFoundError, WorkflowTickError
from loomwork.history.events import (
    Event,
    EventCategory,
    PatchRecorded,
    TickCompleted,
    WorkflowStarted,
    is_closing,
)
from loomwork.runtime.commands import Command, describe_event
from loomwork.runtime.instance import WorkflowInstance
from loomwork.sdk.definitions import Registry


@dataclass(frozen=True, slots=True)
class Divergence:
    """Where and why replay stopped matching the recorded history."""

    event_id: int
    expected: str
    produced: str
    explanation: str


@dataclass(frozen=True, slots=True)
class TickTrace:
    """One activation of the workflow, as it happened during replay."""

    tick_number: int
    tick_event_id: int
    time: datetime
    consumed_through: int
    inputs: list[Event]
    commands: list[Command]
    decisions: list[Event]

    @property
    def command_descriptions(self) -> list[str]:
        return [c.describe() for c in self.commands]

    @property
    def input_summary(self) -> list[str]:
        return [f"{e.kind}#{e.event_id}" for e in self.inputs]


@dataclass(slots=True)
class ReplayTrace:
    """The result of :func:`trace_replay`: every tick up to the stopping point."""

    workflow_name: str
    workflow_id: str
    run_id: str
    source_location: str | None = None
    ticks: list[TickTrace] = field(default_factory=list)
    divergence: Divergence | None = None
    closed_by: Event | None = None
    stopped_at_tick: int | None = None
    query_results: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.divergence is None


def _source_location(registry: Registry, workflow_name: str) -> str | None:
    try:
        fn = inspect.unwrap(registry.workflow(workflow_name).fn)
        path = inspect.getsourcefile(fn)
        _, line = inspect.getsourcelines(fn)
    except (NotFoundError, OSError, TypeError):
        return None
    return f"{path}:{line}" if path else None


def _divergence(
    tick_event_id: int, commands: list[Command], decisions: list[Event]
) -> Divergence | None:
    for index in range(max(len(commands), len(decisions))):
        command = commands[index] if index < len(commands) else None
        decision = decisions[index] if index < len(decisions) else None
        if decision is not None and command is None:
            return Divergence(
                event_id=decision.event_id,
                expected=describe_event(decision),
                produced="(nothing)",
                explanation=(
                    f"history event {decision.event_id} records {describe_event(decision)}, "
                    "but the workflow code did not produce it"
                ),
            )
        if command is not None and decision is None:
            return Divergence(
                event_id=tick_event_id,
                expected="(nothing)",
                produced=command.describe(),
                explanation=(
                    f"the workflow code produced {command.describe()} after tick "
                    f"{tick_event_id}, which is not in the recorded history"
                ),
            )
        assert command is not None
        assert decision is not None
        if not command.matches(decision):
            return Divergence(
                event_id=decision.event_id,
                expected=describe_event(decision),
                produced=command.describe(),
                explanation=(
                    f"history event {decision.event_id} records {describe_event(decision)}, "
                    f"but the workflow code produced {command.describe()}"
                ),
            )
    return None


@dataclass(slots=True)
class _Cursor:
    """Mutable state threaded through one pass over the history."""

    instance: WorkflowInstance
    history: list[Event]
    trace: ReplayTrace
    until_tick: int | None
    pending: list[Event] = field(default_factory=list)
    recorded_patches: set[str] = field(default_factory=set)
    index: int = 0
    tick_number: int = 0

    @property
    def total(self) -> int:
        return len(self.history)


def _collect_decisions(cursor: _Cursor) -> list[Event]:
    decisions: list[Event] = []
    while cursor.index < cursor.total and cursor.history[cursor.index].category is (
        EventCategory.DECISION
    ):
        decision = cursor.history[cursor.index]
        if isinstance(decision, PatchRecorded):
            cursor.recorded_patches.add(decision.patch_id)
        if is_closing(decision):
            cursor.trace.closed_by = decision
        decisions.append(decision)
        cursor.index += 1
    return decisions


def _process_tick(cursor: _Cursor, event: TickCompleted) -> bool:
    """Runs one activation and records it. Returns whether replay must stop here."""
    cursor.tick_number += 1
    batch = [e for e in cursor.pending if e.event_id <= event.consumed_through]
    cursor.pending = [e for e in cursor.pending if e.event_id > event.consumed_through]
    cursor.instance.recorded_patches = cursor.recorded_patches
    cursor.instance.history_length = event.consumed_through
    try:
        commands = cursor.instance.activate(event.time, batch, replaying=True)
    except WorkflowTickError as exc:
        cursor.trace.divergence = Divergence(
            event_id=event.event_id, expected="", produced="", explanation=str(exc)
        )
        return True
    cursor.index += 1
    decisions = _collect_decisions(cursor)
    cursor.trace.ticks.append(
        TickTrace(
            tick_number=cursor.tick_number,
            tick_event_id=event.event_id,
            time=event.time,
            consumed_through=event.consumed_through,
            inputs=batch,
            commands=commands,
            decisions=decisions,
        )
    )
    cursor.trace.divergence = _divergence(event.event_id, commands, decisions)
    if cursor.trace.divergence is not None:
        return True
    if cursor.until_tick is not None and cursor.tick_number >= cursor.until_tick:
        cursor.trace.stopped_at_tick = cursor.tick_number
        return True
    return False


def _run_queries(
    instance: WorkflowInstance, converter: DataConverter, names: list[str], trace: ReplayTrace
) -> None:
    for name in names:
        result = instance.run_query(name, [])
        trace.query_results[name] = converter.decode(result)


def trace_replay(
    history: list[Event],
    registry: Registry,
    *,
    converter: DataConverter | None = None,
    until_tick: int | None = None,
    queries: list[str] | None = None,
) -> ReplayTrace:
    """Replay ``history`` tick by tick, recording a structured trace.

    Stops early -- without raising -- at the first tick whose commands do not match the
    recorded decisions, and always stops after ``until_tick`` activations if given. When
    it stops early (by ``until_tick`` or by reaching the end of the history) with the
    workflow instance still open, ``queries`` are answered against the state as of that
    point, exactly like :meth:`loomwork.engine.queries.QueryRunner.query` does for a
    live run, except with no arguments (the debugger only supports zero-argument
    queries).

    Raises:
        DebugError: the history is empty, does not start a workflow, or names a
            workflow ``registry`` does not define.
    """
    if not history or not isinstance(history[0], WorkflowStarted):
        raise DebugError("history does not start with a WorkflowStarted event")
    started = history[0]
    if not registry.has_workflow(started.workflow_name):
        raise DebugError(f"workflow {started.workflow_name!r} is not defined by the given --app")
    converter = converter or default_converter
    definition = registry.workflow(started.workflow_name)
    instance = WorkflowInstance(definition, started, converter=converter)
    trace = ReplayTrace(
        workflow_name=started.workflow_name,
        workflow_id=started.workflow_id,
        run_id=started.run_id,
        source_location=_source_location(registry, started.workflow_name),
    )
    cursor = _Cursor(instance=instance, history=history, trace=trace, until_tick=until_tick)
    stopped_early = False
    try:
        while cursor.index < cursor.total:
            event = cursor.history[cursor.index]
            if event.is_input:
                if isinstance(event, PatchRecorded):
                    cursor.recorded_patches.add(event.patch_id)
                cursor.pending.append(event)
                cursor.index += 1
                continue
            if isinstance(event, TickCompleted):
                if _process_tick(cursor, event):
                    stopped_early = True
                    break
                continue
            if is_closing(event):
                trace.closed_by = event
            cursor.index += 1

        if queries and trace.divergence is None and (stopped_early or cursor.index >= cursor.total):
            _run_queries(instance, converter, queries, trace)
    finally:
        instance.close()
    return trace
