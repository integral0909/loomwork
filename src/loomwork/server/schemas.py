"""Pydantic request/response schemas for the control API.

The control API is meant for callers without access to Loomwork's Python types --
``curl``, other languages, dashboards -- so arguments and values are plain JSON, encoded
and decoded with :data:`loomwork.codec.converter.default_converter`. The worker API (used
by :class:`~loomwork.remote.http_store.HttpStore`) does not use these: it exchanges the
exact wire form of :mod:`loomwork.store` types via :mod:`loomwork.remote.wire`.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# --------------------------------------------------------------------------- requests


class StartWorkflowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    args: list[Any] = Field(default_factory=list)
    workflow_id: str | None = None
    search_attributes: dict[str, Any] | None = None
    #: Seconds; closes the run after this long regardless of its own logic.
    run_timeout: float | None = None
    #: Seconds; bounds the whole workflow id across retries and continue-as-new.
    execution_timeout: float | None = None
    #: Retries a failed run up to this many attempts; omit for no automatic retry.
    retry_max_attempts: int | None = None
    id_reuse_policy: Literal[
        "allow_duplicate", "allow_duplicate_failed_only", "reject_duplicate"
    ] = "allow_duplicate"
    id_conflict_policy: Literal["fail", "use_existing", "terminate_existing"] = "fail"
    #: Unset resolves from engine configuration, exactly as an unspecified
    #: ``start_workflow`` call would; see ``docs/guides/versioning.md``.
    versioning_policy: Literal["auto_upgrade", "pinned"] | None = None


class ResetWorkflowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    to: Literal["first_tick", "before_last_failure"] | None = None
    after_tick: int | None = None
    reason: str = ""


class SignalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    channel: str
    value: Any = None
    message_id: str = ""


class SignalWithStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    args: list[Any] = Field(default_factory=list)
    workflow_id: str
    channel: str
    value: Any = None
    message_id: str = ""


class RequestUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    args: list[Any] = Field(default_factory=list)
    update_id: str | None = None


class ReasonRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = ""


# --------------------------------------------------------------------------- schedules


class CreateScheduleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schedule_id: str
    workflow: str
    args: list[Any] = Field(default_factory=list)
    cron: str | None = None
    #: Fixed interval, in seconds; give exactly one of ``cron`` and ``interval``.
    interval: float | None = None
    timezone: str = "UTC"
    start_at: str | None = None
    end_at: str | None = None
    jitter: float = 0
    overlap: Literal["skip", "buffer_one", "allow_all", "cancel_other", "terminate_other"] = "skip"
    catchup_window: float = 60
    pause_on_failure: bool = False
    paused: bool = False
    note: str = ""
    #: See IdReusePolicy/IdConflictPolicy on loomwork.start_child; unset reproduces the
    #: behaviour of a schedule created before this field existed (ALLOW_DUPLICATE/FAIL).
    id_reuse_policy: (
        Literal["allow_duplicate", "allow_duplicate_failed_only", "reject_duplicate"] | None
    ) = None
    id_conflict_policy: Literal["fail", "use_existing", "terminate_existing"] | None = None
    #: Unset resolves from engine configuration, exactly as an unspecified
    #: start_workflow call would.
    versioning_policy: Literal["auto_upgrade", "pinned"] | None = None
    #: Seconds; closes each run this schedule starts after this long.
    run_timeout: float | None = None
    #: Seconds; bounds each run's workflow id across its own retries and continue-as-new.
    execution_timeout: float | None = None
    search_attributes: dict[str, Any] | None = None


class UpdateScheduleRequest(BaseModel):
    """Every field is optional; only the ones given are changed."""

    model_config = ConfigDict(extra="forbid")

    cron: str | None = None
    interval: float | None = None
    timezone: str | None = None
    jitter: float | None = None
    overlap: (
        Literal["skip", "buffer_one", "allow_all", "cancel_other", "terminate_other"] | None
    ) = None
    catchup_window: float | None = None
    pause_on_failure: bool | None = None
    note: str | None = None
    id_reuse_policy: (
        Literal["allow_duplicate", "allow_duplicate_failed_only", "reject_duplicate"] | None
    ) = None
    id_conflict_policy: Literal["fail", "use_existing", "terminate_existing"] | None = None
    versioning_policy: Literal["auto_upgrade", "pinned"] | None = None
    run_timeout: float | None = None
    execution_timeout: float | None = None
    search_attributes: dict[str, Any] | None = None


class ScheduleNoteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    note: str = ""


class BackfillRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str
    end: str


# --------------------------------------------------------------------------- builds


class DeclareCompatibleRequest(BaseModel):
    """``POST .../builds/{build_id}/compatible``: let workers of ``build_id`` claim
    runs and pinned-queue steps pinned to ``compatible_with``."""

    model_config = ConfigDict(extra="forbid")

    compatible_with: str


class RepinRequest(BaseModel):
    """``POST .../workflows/{run_id}/repin``: move the run onto ``build_id``, or unpin
    it (``None``) so its next tick pins fresh."""

    model_config = ConfigDict(extra="forbid")

    build_id: str | None = None


# --------------------------------------------------------------------------- responses


class FailureView(BaseModel):
    """JSON view of a :class:`~loomwork.failure.FailureInfo`."""

    model_config = ConfigDict(extra="allow")

    message: str
    type: str
    kind: str = "application"
    non_retryable: bool = False
    details: Any = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    cause: FailureView | None = None


class ExecutionView(BaseModel):
    """JSON view of an :class:`~loomwork.store.records.ExecutionRecord`."""

    model_config = ConfigDict(extra="allow")

    run_id: str
    workflow_id: str
    workflow_name: str
    status: str
    created_at: str
    closed_at: str | None = None
    last_event_id: int = 0
    tick_pending: bool = False
    tick_failures: int = 0
    last_tick_error: str | None = None
    lease_owner: str | None = None
    cancel_requested: bool = False
    result: Any = None
    failure: FailureView | None = None
    close_reason: str = ""
    parent_run_id: str | None = None
    parent_seq: int | None = None
    continued_run_id: str | None = None
    reset_from_run_id: str | None = None
    versioning_policy: str = "auto_upgrade"
    pinned_build_id: str | None = None


class ExecutionListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executions: list[ExecutionView]


class HistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    events: list[dict[str, Any]]


class ChildrenResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    children: list[ExecutionView]


class UpdateAcceptedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    update_id: str


class UpdateOutcomeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    update_id: str
    status: Literal["completed"]
    result: Any = None
    failure: FailureView | None = None


class BoolResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool


class TokenGeneratedResponse(BaseModel):
    """Returned once, at generation time, by operator tooling built on
    :func:`loomwork.server.auth.generate_token`. Never reconstructable afterwards."""

    model_config = ConfigDict(extra="forbid")

    id: str
    token: str
    roles: list[str]
    namespaces: list[str]


class BuildSummaryView(BaseModel):
    """JSON view of a :class:`~loomwork.versioning.builds.BuildSummary`."""

    model_config = ConfigDict(extra="forbid")

    build_id: str
    pinned_running_runs: int
    live_workers: int
    orphaned: bool


class BuildsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    builds: list[BuildSummaryView]


class WorkerPresenceView(BaseModel):
    """JSON view of a :class:`~loomwork.store.records.WorkerPresence`."""

    model_config = ConfigDict(extra="forbid")

    worker_id: str
    build_id: str
    last_seen_at: str
    expires_at: str


class WorkersResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workers: list[WorkerPresenceView]


class BuildCompatibilityView(BaseModel):
    """JSON view of a :class:`~loomwork.store.records.BuildCompatibility`."""

    model_config = ConfigDict(extra="forbid")

    build_id: str
    compatible_with: str
    created_at: str


class BuildCompatibilityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    compatibility: list[BuildCompatibilityView]


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]


class ReadyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "unavailable"]
    namespaces: dict[str, bool] = Field(default_factory=dict)
