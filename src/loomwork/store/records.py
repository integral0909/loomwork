"""Records persisted by stores alongside the event history.

These are *projections*: everything in them can be derived from histories plus the
engine's configuration. They exist so that workers can find work efficiently.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event
from loomwork.options import IdConflictPolicy, IdReusePolicy, StepOptions, VersioningPolicy


class ExecutionStatus(enum.StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TERMINATED = "terminated"
    CONTINUED_AS_NEW = "continued_as_new"
    TIMED_OUT = "timed_out"

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
    parent_run_id: str | None = None
    parent_seq: int | None = None
    parent_close_policy: str = "terminate"
    continued_run_id: str | None = None
    reset_from_run_id: str | None = None
    """Set on a run created by :meth:`~loomwork.engine.engine.Engine.reset`: the run
    whose history it continues from. The reverse link is ``continued_run_id`` on that
    run, reused from continue-as-new (see ``docs/operations/recovery.md``)."""
    history_bytes: int = 0
    """Running total of encoded event body bytes, maintained by the store as events are
    appended. Used for :attr:`~loomwork.engine.config.EngineConfig.history_limit_bytes`."""
    versioning_policy: VersioningPolicy = VersioningPolicy.AUTO_UPGRADE
    """Set once, at creation, from the workflow's configured policy (see
    :meth:`~loomwork.engine.config.EngineConfig.resolve_versioning_policy`). Decides
    whether :attr:`pinned_build_id` is ever populated; see ``docs/adr/
    0006-worker-build-versioning.md``."""
    pinned_build_id: str | None = None
    """For a :attr:`versioning_policy` of ``PINNED``: the build id of the worker that
    committed this run's first tick, set atomically in that tick's commit and never
    changed afterwards except by an explicit :meth:`~loomwork.store.base.Store.repin_run`.
    ``None`` until pinned (a policy of ``PINNED`` whose first tick has not run yet, or
    ran on a worker with no build id), and always ``None`` under ``AUTO_UPGRADE``."""

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
    headers: dict[str, str] = field(default_factory=dict)
    """Copied from the ``StepScheduled`` decision that created this job (propagated
    trace context; see :mod:`loomwork.observability.propagation`)."""
    pinned_build_id: str | None = None
    """Set when this job's queue is one of :attr:`~loomwork.engine.config.EngineConfig.
    pinned_queues` and its run is pinned (see :attr:`ExecutionRecord.pinned_build_id`);
    ``None`` otherwise, meaning any worker may claim it. Steps are not replayed, so
    pinning them is never required for correctness -- only for keeping a step's side
    effects on the same build as the workflow that scheduled it (credentials, binaries
    or drivers a queue's workers hold that a new build changed incompatibly)."""


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    """Answer to a heartbeat: whether the workflow no longer wants the result."""

    cancel_requested: bool


@dataclass(frozen=True, slots=True)
class TimerRecord:
    run_id: str
    seq: int
    fire_at: datetime


class OutboxAction(enum.StrEnum):
    APPEND_INPUT = "append_input"
    REQUEST_CANCEL = "request_cancel"
    TERMINATE = "terminate"


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """A change to *another* run, recorded in the same transaction as its cause.

    The outbox keeps every transaction confined to a single execution row: a child
    reporting to its parent, or a parent terminating its children, writes an outbox
    message instead of locking the other run. A relay applies messages afterwards.

    Exactly one of ``target_run_id`` and ``target_workflow_id`` is set; a workflow id
    addresses that workflow's currently running execution.
    """

    action: OutboxAction
    target_run_id: str | None = None
    target_workflow_id: str | None = None
    event: Event | None = None
    reason: str = ""

    def __post_init__(self) -> None:
        if (self.target_run_id is None) == (self.target_workflow_id is None):
            raise ValueError("exactly one of target_run_id and target_workflow_id is required")
        if self.action is OutboxAction.APPEND_INPUT and (
            self.event is None or not self.event.is_input
        ):
            raise ValueError("APPEND_INPUT messages carry an input event")


@dataclass(frozen=True, slots=True)
class BuildCompatibility:
    """An operator declaration: workers of ``build_id`` may process runs and steps
    pinned to ``compatible_with`` (see :meth:`~loomwork.store.base.Store.
    declare_build_compatible`). Directed: declaring B compatible with A lets B drain A's
    pinned work, not the reverse."""

    build_id: str
    compatible_with: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkerPresence:
    """A worker's most recent build-id heartbeat (see :meth:`~loomwork.store.base.
    Store.heartbeat_worker`). Expired rows (``expires_at <= now``) are not returned by
    :meth:`~loomwork.store.base.Store.list_worker_builds`; they are cheap to leave in
    place and are overwritten or reaped opportunistically rather than needing their own
    cleanup loop."""

    worker_id: str
    build_id: str
    last_seen_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class ChildSpec:
    """An execution to create atomically alongside something else: a child workflow
    (:meth:`~loomwork.store.base.Store.complete_tick`), a scheduled run (:meth:`~loomwork.
    store.base.Store.fire_schedule`), a continuation or a workflow retry.

    ``id_reuse_policy`` and ``id_conflict_policy`` govern ``record.workflow_id`` exactly
    as the same-named parameters of :meth:`~loomwork.store.base.Store.create_execution`
    do for a top-level start; the default (``ALLOW_DUPLICATE``/``FAIL``) reproduces the
    historical behaviour (a running conflict fails to start; reuse is never rejected).
    """

    record: ExecutionRecord
    started: Event
    id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE
    id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL


@dataclass(frozen=True, slots=True)
class CloseOutcome:
    status: ExecutionStatus
    result: Payload | None = None
    failure: FailureInfo | None = None
    reason: str = ""
    continued_run_id: str | None = None


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
    new_children: list[ChildSpec] = field(default_factory=list)
    outbox: list[OutboxMessage] = field(default_factory=list)
    close: CloseOutcome | None = None
    code_version: str = ""
    """Fingerprint of the workflow code that executed the tick."""
    build_id: str | None = None
    """The processing worker's build id, or ``None`` if it declared none. Stamped onto
    the committed ``TickCompleted`` unconditionally; additionally becomes the run's
    ``pinned_build_id`` if its policy is ``PINNED`` and it was not pinned already."""
    search_attributes: dict[str, Any] | None = None
    """Encoded search attribute changes made during the tick (``None`` removes)."""
    continuation: ChildSpec | None = None
    """With a ``CONTINUED_AS_NEW`` close, or a ``FAILED`` close that the workflow's retry
    policy retries: the run to start in the same transaction."""
    continuation_ready_at: datetime | None = None
    """When the continuation may start (a retry's backoff); ``None`` means at once."""
