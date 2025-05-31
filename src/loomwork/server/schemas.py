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
    cancel_requested: bool = False
    result: Any = None
    failure: FailureView | None = None
    close_reason: str = ""
    parent_run_id: str | None = None
    parent_seq: int | None = None


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


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]


class ReadyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "unavailable"]
    namespaces: dict[str, bool] = Field(default_factory=dict)
