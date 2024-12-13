"""Serializable failure records.

Exceptions cannot be persisted directly: their classes may not be importable where the
history is read (another service, the CLI, a newer deploy). Loomwork records failures as
:class:`FailureInfo` trees and reconstructs *Loomwork* exception types from them.
Arbitrary user exceptions come back as :class:`ApplicationError` carrying the original
type name, so callers match on ``error.type`` rather than on classes.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any

from loomwork.errors import (
    ApplicationError,
    CanceledFailure,
    FailureError,
    StepError,
    TimeoutFailure,
)

MAX_CAUSE_DEPTH = 16
MAX_STACK_CHARS = 16_384
MAX_MESSAGE_CHARS = 8_192


@dataclass(frozen=True, slots=True)
class FailureInfo:
    """A recorded failure.

    Attributes:
        message: human-readable description.
        type: stable type identifier (``ApplicationError.type`` or the exception's
            qualified class name).
        kind: which Loomwork error family produced it: ``application``, ``step``,
            ``timeout``, ``canceled`` or ``unknown``.
        stack_trace: formatted traceback of the original exception, truncated.
        non_retryable: whether retry policies must stop immediately.
        details: JSON-compatible extra data supplied by the raiser.
        attributes: kind-specific fields (for ``step``: ``step_name``, ``seq``,
            ``attempts``).
        cause: the failure that caused this one, if any.
    """

    message: str
    type: str
    kind: str = "application"
    stack_trace: str = ""
    non_retryable: bool = False
    details: Any = None
    attributes: dict[str, Any] = field(default_factory=dict)
    cause: FailureInfo | None = None

    # ----------------------------------------------------------------- conversion

    @classmethod
    def from_exception(cls, exc: BaseException, *, _depth: int = 0) -> FailureInfo:
        """Capture ``exc`` (and its ``__cause__`` chain) as a failure record."""
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__, chain=False))
        if len(stack) > MAX_STACK_CHARS:
            stack = stack[: MAX_STACK_CHARS // 2] + "\n...\n" + stack[-MAX_STACK_CHARS // 2 :]
        cause_exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
        cause = None
        if cause_exc is not None and _depth < MAX_CAUSE_DEPTH:
            cause = cls.from_exception(cause_exc, _depth=_depth + 1)

        if isinstance(exc, FailureError):
            if exc.failure is not None and cause is None:
                cause = exc.failure.cause
            attributes: dict[str, Any] = {}
            if isinstance(exc, StepError):
                attributes = {
                    "step_name": exc.step_name,
                    "seq": exc.seq,
                    "attempts": exc.attempts,
                }
            return cls(
                message=_truncate(exc.message),
                type=exc.type,
                kind=_kind_of(exc),
                stack_trace=stack,
                non_retryable=exc.non_retryable,
                details=exc.details,
                attributes=attributes,
                cause=cause,
            )
        return cls(
            message=_truncate(str(exc) or type(exc).__name__),
            type=_qualified_name(type(exc)),
            kind="unknown",
            stack_trace=stack,
            cause=cause,
        )

    def to_exception(self) -> FailureError:
        """Rebuild an exception equivalent to the recorded one."""
        exc: FailureError
        if self.kind == "step":
            exc = StepError(
                self.message,
                step_name=str(self.attributes.get("step_name", "")),
                seq=int(self.attributes.get("seq", 0)),
                attempts=int(self.attributes.get("attempts", 0)),
                type=self.type,
                non_retryable=self.non_retryable,
            )
        elif self.kind == "timeout":
            exc = TimeoutFailure(self.message, type=self.type, non_retryable=self.non_retryable)
        elif self.kind == "canceled":
            exc = CanceledFailure(self.message, type=self.type, non_retryable=self.non_retryable)
        else:
            exc = ApplicationError(
                self.message,
                type=self.type,
                non_retryable=self.non_retryable,
                details=self.details,
            )
        exc.details = self.details
        exc.failure = self
        if self.cause is not None:
            exc.__cause__ = self.cause.to_exception()
        return exc

    # ----------------------------------------------------------------- serialization

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "message": self.message,
            "type": self.type,
            "kind": self.kind,
        }
        if self.stack_trace:
            data["stack_trace"] = self.stack_trace
        if self.non_retryable:
            data["non_retryable"] = True
        if self.details is not None:
            data["details"] = self.details
        if self.attributes:
            data["attributes"] = dict(self.attributes)
        if self.cause is not None:
            data["cause"] = self.cause.to_dict()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FailureInfo:
        cause = data.get("cause")
        return cls(
            message=str(data["message"]),
            type=str(data["type"]),
            kind=str(data.get("kind", "application")),
            stack_trace=str(data.get("stack_trace", "")),
            non_retryable=bool(data.get("non_retryable", False)),
            details=data.get("details"),
            attributes=dict(data.get("attributes", {})),
            cause=cls.from_dict(cause) if cause is not None else None,
        )

    def chain(self) -> list[FailureInfo]:
        """This failure followed by every cause, outermost first."""
        out: list[FailureInfo] = []
        current: FailureInfo | None = self
        while current is not None:
            out.append(current)
            current = current.cause
        return out

    def root_cause(self) -> FailureInfo:
        return self.chain()[-1]


def _kind_of(exc: FailureError) -> str:
    if isinstance(exc, StepError):
        return "step"
    if isinstance(exc, TimeoutFailure):
        return "timeout"
    if isinstance(exc, CanceledFailure):
        return "canceled"
    return "application"


def _qualified_name(cls: type[BaseException]) -> str:
    module = cls.__module__
    if module in {"builtins", "__main__"}:
        return cls.__qualname__
    return f"{module}.{cls.__qualname__}"


def _truncate(message: str) -> str:
    if len(message) <= MAX_MESSAGE_CHARS:
        return message
    return message[:MAX_MESSAGE_CHARS] + "... (truncated)"
