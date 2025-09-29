"""Mapping between Loomwork exceptions and HTTP problem responses.

Shared by the server (which turns an exception raised while handling a request into a
JSON problem body and an HTTP status) and :class:`~loomwork.remote.http_store.HttpStore`
(which turns that body back into the same exception type the embedded stores raise, so
workflow and worker code cannot tell whether it is talking to a local or a remote store).

The body follows the shape of :rfc:`7807` (``type``, ``title``, ``detail``, ``status``)
plus a machine-readable ``code`` and, for a few errors, extra fields needed to rebuild the
exception (for example ``workflow_id`` and ``run_id`` for
:class:`~loomwork.errors.WorkflowAlreadyStartedError`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loomwork.errors import (
    InvalidResetPointError,
    LeaseLostError,
    LoomworkError,
    NotFoundError,
    PayloadError,
    RegistrationError,
    ScheduleConflictError,
    ScheduleExistsError,
    WorkflowAlreadyStartedError,
    WorkflowClosedError,
    WorkflowIdReuseRejectedError,
)
from loomwork.remote.wire import WireFormatError
from loomwork.visibility.errors import QuerySyntaxError, QueryTypeError, VisibilityQueryError
from loomwork.visibility.lexer import Position

#: HTTP status used when no more specific mapping applies.
DEFAULT_STATUS = 500


@dataclass(slots=True)
class ProblemDetail:
    """A JSON problem body, per RFC 7807 plus a ``code`` field."""

    code: str
    title: str
    detail: str
    status: int
    extra: dict[str, Any] = field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        body = {
            "type": f"https://loomwork.dev/errors/{self.code}",
            "code": self.code,
            "title": self.title,
            "detail": self.detail,
            "status": self.status,
        }
        body.update(self.extra)
        return body

    @classmethod
    def from_wire(cls, status: int, data: dict[str, Any]) -> ProblemDetail:
        known = {"type", "code", "title", "detail", "status"}
        return cls(
            code=str(data.get("code", "unknown")),
            title=str(data.get("title", "Error")),
            detail=str(data.get("detail", "")),
            status=int(data.get("status", status)),
            extra={k: v for k, v in data.items() if k not in known},
        )


def problem_for(exc: BaseException) -> ProblemDetail:
    """Build the problem body the server sends for ``exc``."""
    if isinstance(exc, WorkflowAlreadyStartedError):
        return ProblemDetail(
            code="workflow_already_started",
            title="Workflow already started",
            detail=str(exc),
            status=409,
            extra={"workflow_id": exc.workflow_id, "run_id": exc.run_id},
        )
    if isinstance(exc, WorkflowIdReuseRejectedError):
        return ProblemDetail(
            code="workflow_id_reuse_rejected",
            title="Workflow id reuse rejected",
            detail=str(exc),
            status=409,
            extra={
                "workflow_id": exc.workflow_id,
                "run_id": exc.run_id,
                "policy": exc.policy,
            },
        )
    if isinstance(exc, NotFoundError):
        return ProblemDetail(code="not_found", title="Not found", detail=str(exc), status=404)
    if isinstance(exc, InvalidResetPointError):
        return ProblemDetail(
            code="invalid_reset_point", title="Invalid reset point", detail=str(exc), status=422
        )
    if isinstance(exc, LeaseLostError):
        return ProblemDetail(code="lease_lost", title="Lease lost", detail=str(exc), status=409)
    if isinstance(exc, WorkflowClosedError):
        return ProblemDetail(
            code="workflow_closed", title="Workflow closed", detail=str(exc), status=409
        )
    if isinstance(exc, PayloadError):
        return ProblemDetail(
            code="payload_error", title="Invalid payload", detail=str(exc), status=422
        )
    if isinstance(exc, ScheduleExistsError):
        return ProblemDetail(
            code="schedule_exists", title="Schedule already exists", detail=str(exc), status=409
        )
    if isinstance(exc, ScheduleConflictError):
        return ProblemDetail(
            code="schedule_conflict",
            title="Schedule changed concurrently",
            detail=str(exc),
            status=409,
        )
    if isinstance(exc, VisibilityQueryError):
        code = "query_syntax_error" if isinstance(exc, QuerySyntaxError) else "query_type_error"
        return ProblemDetail(
            code=code,
            title="Invalid visibility query",
            detail=exc.format(),
            status=400,
            extra={
                "source": exc.source,
                "line": exc.position.line,
                "column": exc.position.column,
                "offset": exc.position.offset,
                "message": exc.message,
            },
        )
    if isinstance(exc, RegistrationError):
        return ProblemDetail(
            code="registration_error", title="Registration error", detail=str(exc), status=422
        )
    if isinstance(exc, WireFormatError | ValueError | TypeError):
        return ProblemDetail(
            code="invalid_request", title="Invalid request", detail=str(exc), status=400
        )
    if isinstance(exc, TimeoutError):
        return ProblemDetail(code="timeout", title="Timed out", detail=str(exc), status=408)
    if isinstance(exc, LoomworkError):
        return ProblemDetail(
            code="loomwork_error", title=type(exc).__name__, detail=str(exc), status=400
        )
    return ProblemDetail(
        code="internal_error", title="Internal server error", detail="", status=DEFAULT_STATUS
    )


def exception_for(problem: ProblemDetail) -> Exception:
    """Reconstruct the exception a :class:`ProblemDetail` describes.

    Used by the client to raise the same exception type an embedded store would have
    raised, so callers do not need to special-case remote stores.
    """
    if problem.code == "workflow_already_started":
        return WorkflowAlreadyStartedError(
            str(problem.extra.get("workflow_id", "")), str(problem.extra.get("run_id", ""))
        )
    if problem.code == "workflow_id_reuse_rejected":
        return WorkflowIdReuseRejectedError(
            str(problem.extra.get("workflow_id", "")),
            str(problem.extra.get("run_id", "")),
            str(problem.extra.get("policy", "")),
        )
    if problem.code == "not_found":
        return NotFoundError(problem.detail or "not found")
    if problem.code == "invalid_reset_point":
        return InvalidResetPointError(problem.detail or "invalid reset point")
    if problem.code == "lease_lost":
        return LeaseLostError(problem.detail or "lease lost")
    if problem.code == "workflow_closed":
        return WorkflowClosedError(problem.detail or "workflow closed")
    if problem.code == "payload_error":
        return PayloadError(problem.detail or "invalid payload")
    if problem.code == "schedule_exists":
        return ScheduleExistsError(problem.detail or "schedule already exists")
    if problem.code == "schedule_conflict":
        return ScheduleConflictError(problem.detail or "schedule changed concurrently")
    if problem.code in ("query_syntax_error", "query_type_error"):
        position = Position(
            line=int(problem.extra.get("line", 1)),
            column=int(problem.extra.get("column", 1)),
            offset=int(problem.extra.get("offset", 0)),
        )
        message = str(problem.extra.get("message", problem.detail))
        source = str(problem.extra.get("source", ""))
        cls = QuerySyntaxError if problem.code == "query_syntax_error" else QueryTypeError
        return cls(message, source, position)
    if problem.code == "registration_error":
        return RegistrationError(problem.detail or "registration error")
    if problem.code == "invalid_request":
        return ValueError(problem.detail or "invalid request")
    if problem.code == "timeout":
        return TimeoutError(problem.detail or "timed out")
    return LoomworkError(problem.detail or problem.title or "request failed")
