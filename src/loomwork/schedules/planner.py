"""Deciding what a schedule does at a given moment.

:func:`plan` is a pure function: given a schedule's state, the current time and the
statuses of the runs it started, it returns the runs to start, the runs to cancel or
terminate, and the schedule's next state. Keeping the decision logic free of I/O makes
every overlap and catch-up rule unit-testable, and lets the store apply the outcome in
one atomic step.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from loomwork.schedules.model import (
    MAX_RECENT_ACTIONS,
    OverlapPolicy,
    ScheduledRun,
    ScheduleState,
    scheduled_workflow_id,
)
from loomwork.store.records import ExecutionStatus

#: Fire times considered per evaluation; protects against a tiny interval combined
#: with a huge outage.
MAX_FIRE_TIMES = 1000


@dataclass(frozen=True, slots=True)
class PlannedStart:
    nominal_time: datetime
    workflow_id: str


@dataclass(slots=True)
class SchedulePlan:
    state: ScheduleState
    starts: list[PlannedStart] = field(default_factory=list)
    cancel_run_ids: list[str] = field(default_factory=list)
    terminate_run_ids: list[str] = field(default_factory=list)
    skipped: int = 0
    paused_now: bool = False

    @property
    def changes_anything(self) -> bool:
        return bool(self.starts or self.cancel_run_ids or self.terminate_run_ids)


def _next_fire_at(state: ScheduleState, after: datetime) -> datetime | None:
    nominal = state.spec.next_after(after)
    if nominal is None:
        return None
    return nominal + state.spec.jitter_for(state.schedule_id, nominal)


def refresh_running(
    state: ScheduleState, statuses: Mapping[str, ExecutionStatus]
) -> tuple[ScheduleState, bool]:
    """Move closed runs from ``running`` to ``recent``.

    Returns the new state and whether any of the closed runs failed. Runs whose status
    is unknown (for example deleted by retention) are treated as closed.
    """
    still_running: list[ScheduledRun] = []
    recent = list(state.recent)
    failed = False
    for run in state.running:
        status = statuses.get(run.run_id)
        if status is ExecutionStatus.RUNNING:
            still_running.append(run)
            continue
        final = status.value if status is not None else "unknown"
        failed = failed or status is ExecutionStatus.FAILED
        recent = [r for r in recent if r.run_id != run.run_id]
        recent.append(
            ScheduledRun(run.nominal_time, run.started_at, run.workflow_id, run.run_id, final)
        )
    return (
        state.with_(running=tuple(still_running), recent=tuple(recent[-MAX_RECENT_ACTIONS:])),
        failed,
    )


def plan(
    state: ScheduleState,
    now: datetime,
    statuses: Mapping[str, ExecutionStatus],
) -> SchedulePlan:
    """Decide what ``state``'s schedule does at ``now``."""
    state, failed = refresh_running(state, statuses)
    result = SchedulePlan(state=state)
    policy = state.policy

    if failed and policy.pause_on_failure and not state.paused:
        state = state.with_(paused=True, note="paused automatically after a run failed")
        result.paused_now = True

    since = state.last_evaluated or state.created_at
    due = state.spec.fire_times(since, now, MAX_FIRE_TIMES)

    if state.paused:
        # Fire times that pass while paused are skipped, not started on resume.
        result.skipped = len(due)
        result.state = state.with_(
            last_evaluated=now,
            buffered=None,
            next_fire_at=_next_fire_at(state, now),
            total_skipped=state.total_skipped + len(due),
        )
        return result

    window_start = now - policy.catchup_window
    in_window = [t for t in due if t >= window_start]
    skipped = len(due) - len(in_window)
    running = list(state.running)
    buffered = state.buffered
    starts: list[PlannedStart] = []

    def start(nominal: datetime) -> None:
        starts.append(PlannedStart(nominal, scheduled_workflow_id(state.schedule_id, nominal)))

    # A buffered fire time runs as soon as nothing is running any more.
    if buffered is not None and not running:
        start(buffered)
        buffered = None

    if policy.overlap is OverlapPolicy.ALLOW_ALL:
        for nominal in in_window:
            start(nominal)
    elif in_window:
        # Only the most recent fire time matters; earlier ones in the window are skipped.
        skipped += len(in_window) - 1
        latest = in_window[-1]
        busy = bool(running) or bool(starts)
        if not busy:
            start(latest)
        elif policy.overlap is OverlapPolicy.SKIP:
            skipped += 1
        elif policy.overlap is OverlapPolicy.BUFFER_ONE:
            if buffered is not None:
                skipped += 1  # the older buffered fire time is superseded
            buffered = latest
        elif policy.overlap is OverlapPolicy.CANCEL_OTHER:
            result.cancel_run_ids = [r.run_id for r in running]
            start(latest)
        elif policy.overlap is OverlapPolicy.TERMINATE_OTHER:
            result.terminate_run_ids = [r.run_id for r in running]
            running = []
            start(latest)

    result.starts = starts
    result.skipped = skipped
    result.state = state.with_(
        running=tuple(running),
        buffered=buffered,
        last_evaluated=now,
        next_fire_at=_next_fire_at(state, now),
        total_skipped=state.total_skipped + skipped,
    )
    return result


def plan_trigger(state: ScheduleState, now: datetime) -> SchedulePlan:
    """A manual trigger: start one run immediately, ignoring the overlap policy."""
    result = SchedulePlan(state=state)
    result.starts = [PlannedStart(now, scheduled_workflow_id(state.schedule_id, now))]
    return result


def plan_backfill(
    state: ScheduleState, start: datetime, end: datetime, limit: int = MAX_FIRE_TIMES
) -> SchedulePlan:
    """Start one run for every nominal fire time in ``(start, end]`` (runs may overlap)."""
    result = SchedulePlan(state=state)
    times = state.spec.fire_times(start, end, limit)
    result.starts = [PlannedStart(t, scheduled_workflow_id(state.schedule_id, t)) for t in times]
    return result


def record_starts(
    state: ScheduleState, started: list[ScheduledRun], now: datetime
) -> ScheduleState:
    """Add freshly started runs to the state."""
    recent = list(state.recent) + started
    return state.with_(
        running=state.running + tuple(started),
        recent=tuple(recent[-MAX_RECENT_ACTIONS:]),
        total_started=state.total_started + len(started),
        updated_at=now,
    )
