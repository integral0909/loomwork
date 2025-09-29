"""History-truncation logic for :meth:`~loomwork.store.base.Store.reset_run`, shared by
every backend so a reset means exactly the same thing on every store.

A **tick boundary** is where a run's history can be safely cut: the last event of some
tick's committed group (its ``TickCompleted`` plus the decision events it produced).
Resetting keeps every event up to and including one such boundary and drops the rest.
Externally-arriving inputs after that point (signals, update requests, cancellation, a
child run's outcome) are re-delivered so nothing sent to the run is lost; step and timer
*outcomes* are not, so the new run's (fixed) code gets to produce them again rather than
replay the old ones (see ``docs/operations/recovery.md`` for the full semantics and
worked examples).

Reconstructing outstanding steps and timers from history alone (rather than copying the
old run's live store rows) is what makes a reset correct even for a run that has already
*closed*: a closed run's step jobs and timers no longer exist anywhere except as
``StepScheduled``/``TimerStarted`` decision events with no matching outcome.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from loomwork.errors import InvalidResetPointError
from loomwork.history.events import (
    Event,
    EventCategory,
    SearchAttributesUpserted,
    StepCancelRequested,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerCanceled,
    TimerFired,
    TimerStarted,
    WorkflowStarted,
)
from loomwork.options import ResetPoint
from loomwork.store.records import StepJob, TimerRecord
from loomwork.visibility.attributes import merge


@dataclass(frozen=True, slots=True)
class ResetPlan:
    """Everything :meth:`~loomwork.store.base.Store.reset_run` needs, derived purely
    from a run's history: no store access, no clock."""

    cutoff_event_id: int
    """Highest event id kept in the new run (inclusive)."""
    consumed_through: int
    """The new run's ``consumed_through`` once the kept events are copied."""
    redeliver: tuple[Event, ...]
    """External inputs after the cutoff (signals, updates, cancellation, child
    outcomes), re-delivered to the new run as fresh inputs, in their original order.
    Step and timer outcomes are excluded: see the module docstring."""
    open_steps: tuple[StepScheduled, ...]
    """Steps a kept tick scheduled with no outcome recorded within the kept history."""
    open_timers: tuple[TimerStarted, ...]
    """Timers a kept tick started with no outcome recorded within the kept history."""
    search_attributes: dict[str, Any]
    """Encoded search attributes as of the cutoff (the run's initial attributes merged
    with every kept ``SearchAttributesUpserted``)."""


def plan_reset(history: Sequence[Event], to: ResetPoint) -> ResetPlan:
    """Resolve ``to`` against ``history`` (oldest first, as returned by
    :meth:`~loomwork.store.base.Store.load_history`) into a concrete plan.

    Raises:
        InvalidResetPointError: ``to`` does not resolve (an ``after_tick`` count beyond
            the number of ticks the run actually completed).
    """
    if not history or not isinstance(history[0], WorkflowStarted):
        raise ValueError("history must start with WorkflowStarted")
    ticks = [e.event_id for e in history if isinstance(e, TickCompleted)]

    if to.kind == "first_tick":
        tick_index = 0
    elif to.kind == "after_tick":
        assert to.tick_count is not None
        tick_index = to.tick_count
        if tick_index > len(ticks):
            raise InvalidResetPointError(
                f"cannot reset after tick {tick_index}: the run only completed "
                f"{len(ticks)} tick(s)"
            )
    else:
        tick_index = max(len(ticks) - 1, 0)

    if tick_index == 0:
        cutoff = history[0].event_id
        consumed_through = 0
    else:
        tick_event_id = ticks[tick_index - 1]
        completed = next(
            e for e in history if isinstance(e, TickCompleted) and e.event_id == tick_event_id
        )
        consumed_through = completed.consumed_through
        # "After tick N" means a freeze-frame of the history exactly as it stood the
        # moment tick N committed: its TickCompleted plus the contiguous run of decision
        # events it produced (in the same append), and nothing an input arriving even a
        # moment later could add. Anything that arrived afterwards -- even if no further
        # tick had claimed it yet -- belongs to the run's future, not to tick N.
        cutoff = tick_event_id
        for event in history:
            if event.event_id <= tick_event_id:
                continue
            if event.category is not EventCategory.DECISION:
                break
            cutoff = event.event_id

    # Step and timer *outcomes* (StepCompleted, StepFailed, TimerFired) are never
    # redelivered, even when they fall after the cutoff: they are the mechanical result
    # of executing a decision this reset is undoing, and the whole point of resetting is
    # to let the (fixed) code produce that result again, not to replay the old one.
    # Everything else that arrives at a workflow from the outside -- signals, update
    # requests, cancellation, a child run's outcome -- is not reproducible by redoing a
    # local decision, so it is redelivered like any input arriving after a normal tick.
    redeliver = tuple(
        e
        for e in history
        if e.event_id > cutoff
        and e.is_input
        and not isinstance(e, StepCompleted | StepFailed | TimerFired)
    )

    # "Open" (needs a job/timer recreated) means: scheduled by a kept decision, with no
    # outcome recorded within the kept history. Both the schedule and its resolution (an
    # outcome input, or a cancel decision) only count if they themselves fall within the
    # kept history: a resolution made by a tick this reset drops never happened, so the
    # step or timer it would have resolved is open again, to be redone by the new run.
    open_steps: dict[int, StepScheduled] = {}
    open_timers: dict[int, TimerStarted] = {}
    attributes: dict[str, Any] = {}
    for event in history:
        if event.event_id > cutoff:
            continue
        if isinstance(event, StepScheduled):
            open_steps[event.seq] = event
        elif isinstance(event, StepCompleted | StepFailed | StepCancelRequested):
            open_steps.pop(event.seq, None)
        elif isinstance(event, TimerStarted):
            open_timers[event.seq] = event
        elif isinstance(event, TimerFired | TimerCanceled):
            open_timers.pop(event.seq, None)
        elif isinstance(event, WorkflowStarted) and event.search_attributes:
            attributes = merge(attributes, event.search_attributes)
        elif isinstance(event, SearchAttributesUpserted):
            attributes = merge(attributes, event.attributes)

    return ResetPlan(
        cutoff_event_id=cutoff,
        consumed_through=consumed_through,
        redeliver=redeliver,
        open_steps=tuple(open_steps.values()),
        open_timers=tuple(open_timers.values()),
        search_attributes=attributes,
    )


def reconstruct_step_job(
    *,
    run_id: str,
    workflow_id: str,
    workflow_name: str,
    scheduled: StepScheduled,
    now: datetime,
) -> StepJob:
    """The step job a fresh live tick would have created for ``scheduled``, as of ``now``.

    ``scheduled.options`` is the raw (possibly partially unset) options the workflow
    call carried; that is exactly what a live tick records too (engine defaults are
    applied where a field is unset when the job is actually executed, not when it is
    created), so no engine configuration is needed here.

    The job id is derived from ``run_id`` and the step's sequence number rather than
    generated fresh, so every store computes the identical id for the identical reset
    (useful for tooling, and required for the model-based test in
    ``tests/property/test_store_equivalence.py``, which runs the same reset against two
    store implementations and compares their outputs).
    """
    options = scheduled.options
    deadline = now + options.total_timeout if options.total_timeout else None
    return StepJob(
        job_id=f"reset:{run_id}:{scheduled.seq}",
        run_id=run_id,
        workflow_id=workflow_id,
        workflow_name=workflow_name,
        seq=scheduled.seq,
        step_name=scheduled.step_name,
        args=list(scheduled.args),
        options=options,
        scheduled_at=now,
        ready_at=now,
        deadline=deadline,
        queue=options.effective_queue,
        priority=options.effective_priority,
    )


def reconstruct_timer(run_id: str, started: TimerStarted) -> TimerRecord:
    return TimerRecord(run_id=run_id, seq=started.seq, fire_at=started.fire_at)
