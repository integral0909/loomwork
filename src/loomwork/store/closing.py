"""Cross-run consequences of a run closing, shared by every store.

When a run closes, two kinds of other runs may need to hear about it:

* its **parent** receives the outcome as a ``ChildCompleted`` / ``ChildFailed`` input;
* its **running children** are terminated, asked to cancel, or left alone, according to
  the parent-close policy each child was started with.

Both are delivered through the outbox (see :class:`~loomwork.store.records.OutboxMessage`)
so the closing transaction only ever locks the closing run.
"""

from __future__ import annotations

from collections.abc import Collection
from datetime import datetime
from typing import TYPE_CHECKING

from loomwork.failure import FailureInfo
from loomwork.history.events import ChildCompleted, ChildFailed, Event
from loomwork.options import ParentClosePolicy
from loomwork.store.records import (
    ExecutionRecord,
    ExecutionStatus,
    OutboxAction,
    OutboxMessage,
)

if TYPE_CHECKING:
    from loomwork.schedules.model import ScheduleState

CHILD_ALREADY_STARTED = "ChildWorkflowAlreadyStarted"
CHILD_ID_REUSE_REJECTED = "ChildWorkflowIdReuseRejected"


def child_outcome_event(child: ExecutionRecord) -> Event:
    """The input event a parent receives when ``child`` (already closed) finishes."""
    assert child.parent_seq is not None
    return child_outcome_event_for(child.parent_seq, child)


def child_outcome_event_for(seq: int, child: ExecutionRecord) -> Event:
    """Like :func:`child_outcome_event`, for a caller reporting at ``seq`` that is not
    necessarily ``child.parent_seq`` -- a run attached to by :class:`~loomwork.options.
    IdConflictPolicy.USE_EXISTING` after it was already running reports to the attaching
    caller's own sequence number, not to whichever run (if any) actually started it."""
    if child.status is ExecutionStatus.COMPLETED and child.result is not None:
        return ChildCompleted(seq=seq, result=child.result)
    descriptions = {
        ExecutionStatus.FAILED: ("failed", "ChildWorkflowError", "child"),
        ExecutionStatus.CANCELED: ("was cancelled", "ChildWorkflowCanceled", "child"),
        ExecutionStatus.TERMINATED: ("was terminated", "ChildWorkflowTerminated", "child"),
        ExecutionStatus.TIMED_OUT: ("timed out", "ChildWorkflowTimedOut", "child"),
    }
    verb, failure_type, kind = descriptions.get(
        child.status, ("closed without a result", "ChildWorkflowError", "child")
    )
    detail = ""
    if child.failure is not None:
        detail = f": {child.failure.message}"
    elif child.close_reason:
        detail = f": {child.close_reason}"
    failure = FailureInfo(
        message=f"child workflow {child.workflow_name!r} ({child.workflow_id}) {verb}{detail}",
        type=failure_type,
        kind=kind,
        attributes={
            "workflow_id": child.workflow_id,
            "run_id": child.run_id,
            "status": child.status.value,
        },
        cause=child.failure,
    )
    return ChildFailed(seq=seq, failure=failure)


def child_start_failed_event(seq: int, workflow_name: str, workflow_id: str, running: str) -> Event:
    return ChildFailed(
        seq=seq,
        failure=FailureInfo(
            message=(
                f"cannot start child workflow {workflow_name!r}: workflow id {workflow_id!r} "
                f"is already running as {running}"
            ),
            type=CHILD_ALREADY_STARTED,
            kind="child",
            attributes={"workflow_id": workflow_id, "run_id": "", "status": "not_started"},
        ),
    )


def child_id_reuse_rejected_event(
    seq: int, workflow_name: str, workflow_id: str, latest_run_id: str, policy: str
) -> Event:
    """A child could not start because ``policy`` (:class:`~loomwork.options.
    IdReusePolicy`) rejects reusing ``workflow_id`` given its latest (closed) run."""
    return ChildFailed(
        seq=seq,
        failure=FailureInfo(
            message=(
                f"cannot start child workflow {workflow_name!r}: workflow id {workflow_id!r} "
                f"cannot be reused under {policy}: its latest run {latest_run_id} does not "
                "qualify"
            ),
            type=CHILD_ID_REUSE_REJECTED,
            kind="child",
            attributes={"workflow_id": workflow_id, "run_id": "", "status": "not_started"},
        ),
    )


def closing_messages(
    closed: ExecutionRecord, running_children: list[ExecutionRecord]
) -> list[OutboxMessage]:
    """Outbox messages to enqueue in the transaction that closed ``closed``."""
    messages: list[OutboxMessage] = []
    continued = closed.continued_run_id is not None
    # A run that continued as new, or failed and was retried, hands its place in the
    # parent to its successor, which reports when the chain truly closes.
    if closed.parent_run_id is not None and closed.parent_seq is not None and not continued:
        messages.append(
            OutboxMessage(
                action=OutboxAction.APPEND_INPUT,
                target_run_id=closed.parent_run_id,
                event=child_outcome_event(closed),
            )
        )
    for child in running_children:
        policy = ParentClosePolicy(child.parent_close_policy)
        reason = f"parent run {closed.run_id} closed ({closed.status.value})"
        if policy is ParentClosePolicy.TERMINATE:
            messages.append(
                OutboxMessage(OutboxAction.TERMINATE, target_run_id=child.run_id, reason=reason)
            )
        elif policy is ParentClosePolicy.REQUEST_CANCEL:
            messages.append(
                OutboxMessage(
                    OutboxAction.REQUEST_CANCEL, target_run_id=child.run_id, reason=reason
                )
            )
    return messages


def drop_unstarted(
    state: ScheduleState, conflicts: set[str], *, reuse_rejected: Collection[str] = ()
) -> ScheduleState:
    """Remove runs that could not be created from a schedule state being saved.

    ``conflicts`` lost to :class:`~loomwork.options.IdConflictPolicy.FAIL` (the schedule
    equivalent of :class:`~loomwork.errors.WorkflowAlreadyStartedError`) are recorded as
    ``already_running``; ``reuse_rejected`` (a subset of ``conflicts``) lost to
    :class:`~loomwork.options.IdReusePolicy` instead, and are recorded as
    ``id_reuse_rejected``. See :func:`attach_unstarted` for ``USE_EXISTING``, which
    adopts a run instead of dropping it.
    """
    from loomwork.schedules.model import ScheduledRun

    if not conflicts:
        return state
    running = tuple(r for r in state.running if r.run_id not in conflicts)
    recent = tuple(
        ScheduledRun(
            r.nominal_time,
            r.started_at,
            r.workflow_id,
            r.run_id,
            "id_reuse_rejected" if r.run_id in reuse_rejected else "already_running",
        )
        if r.run_id in conflicts
        else r
        for r in state.recent
    )
    return state.with_(
        running=running,
        recent=recent,
        total_started=state.total_started - len(conflicts),
    )


def attach_unstarted(state: ScheduleState, attached: dict[str, str]) -> ScheduleState:
    """Rewrite the planned run id of every ``USE_EXISTING`` attachment to the run id of
    the execution it actually attached to (see :func:`drop_unstarted` for the run ids
    that were dropped instead of attached).

    ``attached`` maps the run id the schedule *planned* to create (never created, since
    an execution was already running for that fire time's workflow id) to the run id it
    was told about instead. The schedule tracks and reports on the latter from here on.
    """
    from loomwork.schedules.model import ScheduledRun

    if not attached:
        return state

    def rewrite(r: ScheduledRun) -> ScheduledRun:
        actual = attached.get(r.run_id)
        if actual is None:
            return r
        return ScheduledRun(r.nominal_time, r.started_at, r.workflow_id, actual, r.status)

    return state.with_(
        running=tuple(rewrite(r) for r in state.running),
        recent=tuple(rewrite(r) for r in state.recent),
    )


def reconcile_running(state: ScheduleState, terminated: Collection[str] = ()) -> ScheduleState:
    """Keep ``state.running`` an accurate, deduplicated picture of what a schedule is
    tracking right now, immediately after id conflict resolution.

    Without this, two things could leave stale entries in ``running`` until the
    schedule's *next* evaluation happens to notice and clean them up (see
    :func:`~loomwork.schedules.planner.refresh_running`):

    * ``USE_EXISTING`` may rewrite (:func:`attach_unstarted`) a freshly planned run id
      onto one the schedule is already tracking (two fire times resolving to the same
      run, most commonly the exact same nominal time evaluated twice) -- without
      deduplicating, that run would appear twice.
    * ``TERMINATE_EXISTING`` terminates a tracked run in the very same transaction that
      starts its replacement; ``terminated`` lets that run's entry be retired here
      rather than left claiming ``status="running"`` until it is not.
    """
    from loomwork.schedules.model import ScheduledRun

    if not state.running:
        return state
    terminated_set = set(terminated)
    seen: set[str] = set()
    running: list[ScheduledRun] = []
    for run in reversed(state.running):  # keep the most recent occurrence of each run id
        if run.run_id in terminated_set or run.run_id in seen:
            continue
        seen.add(run.run_id)
        running.append(run)
    running.reverse()
    if len(running) == len(state.running) and not terminated_set:
        return state
    recent = tuple(
        ScheduledRun(r.nominal_time, r.started_at, r.workflow_id, r.run_id, "terminated")
        if r.run_id in terminated_set and r.status == "running"
        else r
        for r in state.recent
    )
    return state.with_(running=tuple(running), recent=recent)


#: Reserved timer sequence number for a run's deadline (workflow timers start at 1).
DEADLINE_TIMER_SEQ = 0


def run_deadline(started: Event, created_at: datetime) -> datetime | None:
    """When the run started by ``started`` times out, if it has a timeout."""
    from datetime import timedelta

    run_timeout = getattr(started, "run_timeout", None)
    execution_deadline = getattr(started, "execution_deadline", None)
    candidates = [d for d in (execution_deadline,) if d is not None]
    if run_timeout is not None:
        candidates.append(created_at + timedelta(seconds=run_timeout))
    return min(candidates) if candidates else None
