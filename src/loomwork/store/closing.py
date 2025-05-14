"""Cross-run consequences of a run closing, shared by every store.

When a run closes, two kinds of other runs may need to hear about it:

* its **parent** receives the outcome as a ``ChildCompleted`` / ``ChildFailed`` input;
* its **running children** are terminated, asked to cancel, or left alone, according to
  the parent-close policy each child was started with.

Both are delivered through the outbox (see :class:`~loomwork.store.records.OutboxMessage`)
so the closing transaction only ever locks the closing run.
"""

from __future__ import annotations

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


def child_outcome_event(child: ExecutionRecord) -> Event:
    """The input event a parent receives when ``child`` (already closed) finishes."""
    assert child.parent_seq is not None
    if child.status is ExecutionStatus.COMPLETED and child.result is not None:
        return ChildCompleted(seq=child.parent_seq, result=child.result)
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
    return ChildFailed(seq=child.parent_seq, failure=failure)


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


def drop_unstarted(state: ScheduleState, conflicts: set[str]) -> ScheduleState:
    """Remove runs that could not be created from a schedule state being saved."""
    from loomwork.schedules.model import ScheduledRun

    if not conflicts:
        return state
    running = tuple(r for r in state.running if r.run_id not in conflicts)
    recent = tuple(
        ScheduledRun(r.nominal_time, r.started_at, r.workflow_id, r.run_id, "already_running")
        if r.run_id in conflicts
        else r
        for r in state.recent
    )
    return state.with_(
        running=running,
        recent=recent,
        total_started=state.total_started - len(conflicts),
    )


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
