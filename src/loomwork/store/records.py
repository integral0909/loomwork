"""Records persisted by stores alongside the event history.

These are *projections*: everything in them can be derived from histories plus the
engine's configuration. They exist so that workers can find work efficiently.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event
from loomwork.options import StepOptions


class ExecutionStatus(enum.StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TERMINATED = "terminated"

    @property
    def is_closed(self) -> bool:
        return self is not ExecutionStatus.RUNNING


@dataclass(slots=True)
class ExecutionRecord:
    """The state of one workflow run.

    Attributes:
        last_event_id: id of the newest event in the history.
        last_input_id: id of the newest *input* event; a tick is pending whenever it is
            greater than ``consumed_through``.
        consumed_through: highest input id delivered by a committed tick.
        tick_pending: a tick should run (derived from the two fields above, stored for
            efficient claiming).
        tick_ready_at: earliest time the pending tick may be claimed (used for backoff
            after tick failures).
        tick_failures: consecutive failed tick attempts; reset by a successful tick.
        last_tick_error: message of the most recent tick failure, for operators.
    """

    run_id: str
    workflow_id: str
    workflow_name: str
    status: ExecutionStatus
    created_at: datetime
    closed_at: datetime | None = None
    last_event_id: int = 0
    last_input_id: int = 0
    consumed_through: int = 0
    tick_pending: bool = False
    tick_ready_at: datetime | None = None
    tick_failures: int = 0
    last_tick_error: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    cancel_requested: bool = False
    result: Payload | None = None
    failure: FailureInfo | None = None
    close_reason: str = ""

    @property
    def is_closed(self) -> bool:
        return self.status.is_closed


@dataclass(slots=True)
class StepJob:
    """A scheduled step waiting for (or undergoing) execution.

    ``attempt`` counts attempts *started*; the store increments it on every claim, so a
    job whose worker died mid-attempt comes back with the next attempt number.

    ``queue`` and ``priority`` are copied from the resolved step options so that claims
    can filter and order on indexed columns. ``heartbeat_details`` is the payload of the
    most recent :func:`loomwork.heartbeat` call; it survives retries so a new attempt
    can resume where the previous one stopped.
    """

    job_id: str
    run_id: str
    workflow_id: str
    workflow_name: str
    seq: int
    step_name: str
    args: list[Payload]
    options: StepOptions
    scheduled_at: datetime
    ready_at: datetime
    deadline: datetime | None = None
    attempt: int = 0
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    last_failure: FailureInfo | None = None
    cancel_requested: bool = False
    queue: str = "default"
    priority: int = 0
    heartbeat_details: Payload | None = None
    last_heartbeat_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    """Answer to a heartbeat: whether the workflow no longer wants the result."""

    cancel_requested: bool


@dataclass(frozen=True, slots=True)
class TimerRecord:
    run_id: str
    seq: int
    fire_at: datetime


@dataclass(frozen=True, slots=True)
class CloseOutcome:
    status: ExecutionStatus
    result: Payload | None = None
    failure: FailureInfo | None = None
    reason: str = ""


@dataclass(slots=True)
class TickOutcome:
    """Everything a completed tick changes, committed atomically by the store."""

    worker_id: str
    consumed_through: int
    time: datetime
    decisions: list[Event] = field(default_factory=list)
    new_steps: list[StepJob] = field(default_factory=list)
    cancel_steps: list[int] = field(default_factory=list)
    new_timers: list[TimerRecord] = field(default_factory=list)
    cancel_timers: list[int] = field(default_factory=list)
    close: CloseOutcome | None = None
