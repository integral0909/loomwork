"""Replaying a history into a :class:`WorkflowInstance` (see ADR-0002).

A :class:`Replayer` is fed history events in order, possibly across several calls (a
worker caches replayers between ticks and only feeds the events it has not seen). For
every ``TickCompleted`` it delivers the inputs that tick consumed, then checks that the
decision events recorded after it match the commands the code produced.

When the history is exhausted, :meth:`Replayer.live_tick` runs a new activation with every
input not yet consumed and returns the resulting commands for the engine to commit. Once
committed, :meth:`Replayer.acknowledge` registers the tick's ``TickCompleted`` id so that
the same tick is skipped (not re-executed) when its events are fed back later.
"""

from __future__ import annotations

import collections
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime

from loomwork.errors import NondeterminismError, WorkflowTickError
from loomwork.history.events import (
    Event,
    EventCategory,
    TickCompleted,
    WorkflowStarted,
    is_closing,
)
from loomwork.runtime.commands import Command, describe_event
from loomwork.runtime.instance import WorkflowInstance

InstanceFactory = Callable[[WorkflowStarted], WorkflowInstance]


class HistoryGapError(WorkflowTickError):
    """Events were fed out of order or with ids missing."""


@dataclass(frozen=True, slots=True)
class LiveTick:
    """Result of running a new activation."""

    commands: list[Command]
    consumed_through: int
    time: datetime
    delivered: int

    @property
    def is_empty(self) -> bool:
        """Nothing was delivered and nothing was decided: no need to record a tick."""
        return self.delivered == 0 and not self.commands


class Replayer:
    """Drives a workflow instance from its history."""

    def __init__(self, factory: InstanceFactory) -> None:
        self._factory = factory
        self.instance: WorkflowInstance | None = None
        self.last_event_id = 0
        self.consumed_through = 0
        self.last_tick_time: datetime | None = None
        self.closed_by: Event | None = None
        self.replayed_ticks = 0
        self._pending: list[Event] = []
        self._expected: collections.deque[Command] = collections.deque()
        self._own_ticks: set[int] = set()
        self._skipping_own = False

    # ------------------------------------------------------------------ feeding

    def feed(self, events: Iterable[Event]) -> None:
        """Apply history events. Already-seen events are ignored."""
        for event in events:
            if event.event_id <= self.last_event_id:
                continue
            if event.event_id != self.last_event_id + 1:
                raise HistoryGapError(
                    f"expected event {self.last_event_id + 1}, got {event.event_id}"
                )
            self._apply(event)
            self.last_event_id = event.event_id
        # Decisions of the last tick may still be on their way (paged history loads),
        # so unmatched commands are only an error once something else follows them:
        # the next non-decision event, a live tick, or the end of a full replay.

    def _apply(self, event: Event) -> None:
        if event.category is EventCategory.DECISION:
            self._apply_decision(event)
            return

        # Any non-decision event ends the decision run of the previous tick.
        self._ensure_decisions_matched()
        self._skipping_own = False

        if event.category is EventCategory.INPUT:
            if isinstance(event, WorkflowStarted) and self.instance is None:
                self.instance = self._factory(event)
            self._pending.append(event)
        elif isinstance(event, TickCompleted):
            self._apply_tick(event)
        elif is_closing(event):
            self.closed_by = event

    def _apply_tick(self, tick: TickCompleted) -> None:
        if tick.event_id in self._own_ticks:
            # This worker executed the tick live and already holds its state.
            self._own_ticks.discard(tick.event_id)
            self._skipping_own = True
            self._drop_consumed(tick.consumed_through)
            self._advance(tick)
            return
        instance = self._require_instance()
        batch = self._drop_consumed(tick.consumed_through)
        commands = instance.activate(tick.time, batch, replaying=True)
        self._expected.extend(commands)
        self.replayed_ticks += 1
        self._advance(tick)

    def _advance(self, tick: TickCompleted) -> None:
        self.consumed_through = max(self.consumed_through, tick.consumed_through)
        self.last_tick_time = tick.time

    def _drop_consumed(self, through: int) -> list[Event]:
        batch = [e for e in self._pending if e.event_id <= through]
        self._pending = [e for e in self._pending if e.event_id > through]
        return batch

    def _apply_decision(self, event: Event) -> None:
        if is_closing(event):
            self.closed_by = event
        if self._skipping_own:
            return
        if not self._expected:
            raise NondeterminismError(
                f"history event {event.event_id} records {describe_event(event)}, "
                "but the workflow code did not produce it"
            )
        command = self._expected.popleft()
        if not command.matches(event):
            raise NondeterminismError(
                f"history event {event.event_id} records {describe_event(event)}, "
                f"but the workflow code produced {command.describe()}"
            )

    def _ensure_decisions_matched(self) -> None:
        if self._expected:
            command = self._expected[0]
            raise NondeterminismError(
                f"the workflow code produced {command.describe()} after event "
                f"{self.last_event_id}, which is not in the recorded history"
            )

    def _require_instance(self) -> WorkflowInstance:
        if self.instance is None:
            raise WorkflowTickError("history does not start with WorkflowStarted")
        return self.instance

    # ------------------------------------------------------------------ live execution

    @property
    def is_closed(self) -> bool:
        return self.closed_by is not None

    @property
    def pending_inputs(self) -> list[Event]:
        return list(self._pending)

    def live_tick(self, now: datetime) -> LiveTick:
        """Run a new activation with every unconsumed input.

        ``now`` is clamped so workflow time never moves backwards.
        """
        instance = self._require_instance()
        self._ensure_decisions_matched()
        if self.closed_by is not None:
            raise WorkflowTickError("the run is already closed")
        time = now if self.last_tick_time is None else max(now, self.last_tick_time)
        batch, self._pending = self._pending, []
        commands = instance.activate(time, batch, replaying=False)
        consumed = max((e.event_id for e in batch), default=self.consumed_through)
        self.consumed_through = consumed
        self.last_tick_time = time
        return LiveTick(
            commands=commands, consumed_through=consumed, time=time, delivered=len(batch)
        )

    def finish(self) -> None:
        """Assert the history fed so far is complete: no command awaits its decision."""
        self._ensure_decisions_matched()

    def acknowledge(self, tick_event_id: int) -> None:
        """Record that the tick committed as ``tick_event_id`` was executed live here."""
        self._own_ticks.add(tick_event_id)

    def close(self) -> None:
        if self.instance is not None:
            self.instance.close()


def replay_history(events: Iterable[Event], factory: InstanceFactory) -> Replayer:
    """Replay a complete history, raising :class:`NondeterminismError` on mismatch.

    Useful for testing that new workflow code is compatible with recorded runs.
    """
    replayer = Replayer(factory)
    replayer.feed(events)
    replayer.finish()
    return replayer
