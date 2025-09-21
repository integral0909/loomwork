"""Tick-by-tick replay tracing: successful replay, until-tick + queries, and divergence."""

from __future__ import annotations

import inspect

import pytest

import loomwork as lw
from loomwork import ManualClock, MemoryStore
from loomwork.debug import DebugError, trace_replay
from loomwork.history.events import WorkflowCompleted
from loomwork.sdk.definitions import Registry
from tests.debug import sample
from tests.helpers import eventually


async def _completed_history(make_app, memory_store: MemoryStore):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    handle = await app.start_workflow(sample.approval, "widget", id="a1")
    await eventually(lambda: memory_store.timer_count == 0)
    await handle.signal(sample.decisions, "approved")
    result = await handle.result(timeout=5)
    assert result == "WIDGET: approved"
    return await handle.history(), app.registry, app.converter


async def test_trace_replay_matches_full_history(
    make_app, memory_store: MemoryStore, clock: ManualClock
):
    history, registry, converter = await _completed_history(make_app, memory_store)

    trace = trace_replay(history, registry, converter=converter)

    assert trace.ok
    assert trace.divergence is None
    assert trace.workflow_name == "dbg.approval"
    assert trace.workflow_id == "a1"
    assert isinstance(trace.closed_by, WorkflowCompleted)
    assert len(trace.ticks) >= 2
    first_tick = trace.ticks[0]
    assert first_tick.tick_number == 1
    assert any("ScheduleStep" in d for d in first_tick.command_descriptions)
    # Every decision recorded in the history was matched by a produced command.
    for tick in trace.ticks:
        assert len(tick.commands) == len(tick.decisions)


async def test_source_location_points_at_the_workflow_function(
    make_app, memory_store: MemoryStore, clock: ManualClock
):
    history, registry, converter = await _completed_history(make_app, memory_store)
    trace = trace_replay(history, registry, converter=converter)

    assert trace.source_location is not None
    path, _, line = trace.source_location.rpartition(":")
    assert path == inspect.getsourcefile(sample.approval.fn)
    assert int(line) == inspect.getsourcelines(sample.approval.fn)[1]


async def test_until_tick_stops_early_and_answers_queries(
    make_app, memory_store: MemoryStore, clock: ManualClock
):
    history, registry, converter = await _completed_history(make_app, memory_store)

    trace = trace_replay(history, registry, converter=converter, until_tick=2, queries=["state"])

    assert trace.ok
    assert trace.stopped_at_tick == 2
    assert len(trace.ticks) == 2
    assert trace.query_results == {"state": {"item": "WIDGET"}}


def _changed_registry() -> Registry:
    """A second version of ``dbg.approval`` that calls a differently named step."""

    @lw.step(name="dbg.whisper")
    async def whisper(text: str) -> str:
        return text.lower()

    @lw.workflow(name="dbg.approval")
    async def approval_v2(item: str) -> str:
        loud = await whisper(item)
        decision = await sample.decisions.receive()
        return f"{loud}: {decision}"

    return Registry([approval_v2], [whisper])


async def test_divergence_reports_expected_and_produced(
    make_app, memory_store: MemoryStore, clock: ManualClock
):
    history, _registry, converter = await _completed_history(make_app, memory_store)

    trace = trace_replay(history, _changed_registry(), converter=converter)

    assert not trace.ok
    assert trace.divergence is not None
    assert "dbg.step" in trace.divergence.expected
    assert "dbg.whisper" in trace.divergence.produced
    assert "history event" in trace.divergence.explanation
    # Ticks before the divergence still replayed and were recorded.
    assert trace.ticks
    assert all(t.tick_event_id < trace.divergence.event_id for t in trace.ticks)


async def test_empty_history_raises_debug_error():
    with pytest.raises(DebugError, match="WorkflowStarted"):
        trace_replay([], Registry([], []))


async def test_unknown_workflow_raises_debug_error(
    make_app, memory_store: MemoryStore, clock: ManualClock
):
    history, _registry, converter = await _completed_history(make_app, memory_store)
    empty_registry = Registry([], [])
    with pytest.raises(DebugError, match="dbg.approval"):
        trace_replay(history, empty_registry, converter=converter)
