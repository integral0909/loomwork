"""Structured access to, and fluent assertions about, recorded workflow histories."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeAlias

from loomwork.client.handle import WorkflowHandle
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.history.events import (
    Event,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerFired,
    WorkflowCanceled,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowStarted,
)
from loomwork.runtime.commands import describe_event
from loomwork.sdk.definitions import Registry, WorkflowDefinition
from loomwork.versioning.compat import check_history


class HistoryView:
    """A run's history, ready for pretty-printing or programmatic inspection.

    Iterate it, index it or call :meth:`pretty` for a debugging dump; use
    :func:`assert_history` for assertions.
    """

    def __init__(self, events: Sequence[Event]) -> None:
        self.events: list[Event] = list(events)

    def __len__(self) -> int:
        return len(self.events)

    def __iter__(self) -> Any:
        return iter(self.events)

    def __getitem__(self, index: int) -> Event:
        return self.events[index]

    def kinds(self) -> list[str]:
        """The ``kind`` of every event, in order -- a quick shape-of-history check."""
        return [event.kind for event in self.events]

    def of_kind(self, kind: str) -> list[Event]:
        return [event for event in self.events if event.kind == kind]

    def pretty(self) -> str:
        """A multi-line, human-readable rendering, one line per event."""
        lines = [
            f"{event.event_id:>4}  {event.kind:<26} {_describe(event)}" for event in self.events
        ]
        return "\n".join(lines)


#: Anything :func:`assert_history` and the golden-history helpers accept as a source of
#: events: a live handle, a history already fetched from one, or plain events (for
#: example replayed from a golden file with :func:`~loomwork.history.history_from_json`).
HistorySource: TypeAlias = WorkflowHandle[Any] | HistoryView | Sequence[Event]  # noqa: UP040


def _describe(event: Event) -> str:
    if event.is_decision:
        return describe_event(event)
    if isinstance(event, WorkflowStarted):
        return f"workflow={event.workflow_name!r} workflow_id={event.workflow_id!r}"
    if isinstance(event, StepCompleted):
        return f"seq={event.seq} attempts={event.attempts}"
    if isinstance(event, StepFailed):
        return f"seq={event.seq} attempts={event.attempts} failure={event.failure.type!r}"
    if isinstance(event, TimerFired):
        return f"seq={event.seq}"
    if isinstance(event, TickCompleted):
        return f"consumed_through={event.consumed_through}"
    seq = getattr(event, "seq", None)
    return f"seq={seq}" if seq is not None else ""


async def events_of(source: HistorySource) -> list[Event]:
    """Resolve any :data:`HistorySource` to a plain list of events."""
    if isinstance(source, HistoryView):
        return list(source.events)
    if isinstance(source, WorkflowHandle):
        return await source.history()
    return list(source)


class HistoryAssertion:
    """Fluent assertions over a recorded history.

    Every method raises :class:`AssertionError` (with the mismatch spelled out) on
    failure and returns ``self`` on success, so checks can be chained::

        assertion = await assert_history(handle)
        assertion.contains_step("charge_card", times=1).completed_with(receipt)
    """

    def __init__(
        self,
        events: Sequence[Event],
        *,
        registry: Registry | None = None,
        converter: DataConverter | None = None,
    ) -> None:
        self.events: list[Event] = list(events)
        self._registry = registry
        self._converter = converter or default_converter

    @property
    def view(self) -> HistoryView:
        return HistoryView(self.events)

    def contains_step(self, name: str, *, times: int | None = None) -> HistoryAssertion:
        """Assert that a step named ``name`` was scheduled, optionally exactly ``times``
        times (once per call site the workflow reached, regardless of retries)."""
        count = sum(1 for e in self.events if isinstance(e, StepScheduled) and e.step_name == name)
        if count == 0:
            raise AssertionError(f"step {name!r} was never scheduled in this history")
        if times is not None and count != times:
            raise AssertionError(
                f"step {name!r} was scheduled {count} time(s) in this history, expected {times}"
            )
        return self

    def step_failed(self, name: str) -> HistoryAssertion:
        """Assert that a step named ``name`` recorded a (permanent) failure."""
        scheduled = {e.seq: e.step_name for e in self.events if isinstance(e, StepScheduled)}
        if any(isinstance(e, StepFailed) and scheduled.get(e.seq) == name for e in self.events):
            return self
        raise AssertionError(f"step {name!r} never recorded a failure in this history")

    def step_completed(self, name: str, *, times: int | None = None) -> HistoryAssertion:
        """Assert that a step named ``name`` completed successfully, optionally exactly
        ``times`` times."""
        scheduled = {e.seq: e.step_name for e in self.events if isinstance(e, StepScheduled)}
        count = sum(
            1 for e in self.events if isinstance(e, StepCompleted) and scheduled.get(e.seq) == name
        )
        if count == 0:
            raise AssertionError(f"step {name!r} never completed in this history")
        if times is not None and count != times:
            raise AssertionError(
                f"step {name!r} completed {count} time(s) in this history, expected {times}"
            )
        return self

    def completed_with(self, value: Any) -> HistoryAssertion:
        """Assert that the run completed and decoded its result equal to ``value``."""
        for event in self.events:
            if isinstance(event, WorkflowCompleted):
                hint = type(value) if value is not None else Any
                actual = self._converter.decode(event.result, hint)
                if actual != value:
                    raise AssertionError(f"workflow completed with {actual!r}, expected {value!r}")
                return self
        raise AssertionError(f"workflow did not complete: {self._outcome_summary()}")

    def failed_with(self, exc_type: type[BaseException]) -> HistoryAssertion:
        """Assert that the run failed with a failure matching ``exc_type``."""
        for event in self.events:
            if isinstance(event, WorkflowFailed):
                exc = event.failure.to_exception()
                if not isinstance(exc, exc_type):
                    raise AssertionError(
                        f"workflow failed with {type(exc).__name__} ({event.failure.message!r}), "
                        f"expected {exc_type.__name__}"
                    )
                return self
        raise AssertionError(f"workflow did not fail: {self._outcome_summary()}")

    def canceled(self) -> HistoryAssertion:
        """Assert that the run was canceled."""
        if any(isinstance(e, WorkflowCanceled) for e in self.events):
            return self
        raise AssertionError(f"workflow was not canceled: {self._outcome_summary()}")

    def no_nondeterminism(self, *workflows: WorkflowDefinition[..., Any]) -> HistoryAssertion:
        """Assert that this history still replays cleanly against ``workflows``.

        If no workflows are given, the registry this assertion was built with (when it
        came from :meth:`WorkflowEnvironment.assert_history
        <loomwork.testing.WorkflowEnvironment.assert_history>`) is used instead.
        """
        registry = Registry(workflows, ()) if workflows else self._registry
        if registry is None:
            raise AssertionError(
                "no_nondeterminism() needs workflow definitions: pass them as arguments, or "
                "build this assertion through WorkflowEnvironment.assert_history()"
            )
        check = check_history(self.events, registry, converter=self._converter)
        if check.nondeterministic:
            raise AssertionError(f"history diverges from the given workflow code: {check.error}")
        if not check.ok:
            raise AssertionError(f"history could not be replayed: {check.error}")
        return self

    def _outcome_summary(self) -> str:
        tail = ", ".join(e.kind for e in self.events[-3:]) or "(empty history)"
        return f"last events were: {tail}"


async def assert_history(
    source: HistorySource,
    *,
    registry: Registry | None = None,
    converter: DataConverter | None = None,
) -> HistoryAssertion:
    """Fetch (if needed) and wrap a history for fluent assertions.

    ``source`` is a :class:`~loomwork.client.handle.WorkflowHandle`, an already-fetched
    :class:`HistoryView`, or a plain sequence of events.
    """
    events = await events_of(source)
    return HistoryAssertion(events, registry=registry, converter=converter)
