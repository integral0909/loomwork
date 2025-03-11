"""Exception hierarchy.

Errors fall into three groups:

* **Durable failures** (:class:`FailureError` subclasses) are part of a workflow's recorded
  outcome. Raising one out of a workflow fails the run; receiving one from a step is a
  normal, replayable event.
* **Engine errors** describe misuse of the client or engine API (unknown workflow,
  duplicate id, ...). They never enter a history.
* **Tick errors** (:class:`NondeterminismError`, :class:`WorkflowTickError`) mean a tick
  could not be completed. The run is not failed; the tick is retried later so that a
  fixed deployment can pick it up.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loomwork.failure import FailureInfo


class LoomworkError(Exception):
    """Base class for all errors raised by Loomwork."""


# --------------------------------------------------------------------------- durable


class FailureError(LoomworkError):
    """An error that can be recorded in a history and reconstructed on replay."""

    def __init__(
        self,
        message: str,
        *,
        type: str | None = None,
        non_retryable: bool = False,
        details: object = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.type = type or self.__class__.__name__
        self.non_retryable = non_retryable
        self.details = details
        self.failure: FailureInfo | None = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(type={self.type!r}, message={self.message!r})"


class ApplicationError(FailureError):
    """Raised by user code to signal a business-level failure.

    ``type`` is a free-form, stable identifier (for example ``"CardDeclined"``) that
    retry policies and callers can match on without importing the raising module.
    """


class StepError(FailureError):
    """A step failed after exhausting its retry policy.

    The underlying error is available as ``__cause__``.
    """

    def __init__(
        self,
        message: str,
        *,
        step_name: str,
        seq: int,
        attempts: int,
        type: str | None = None,
        non_retryable: bool = False,
    ) -> None:
        super().__init__(message, type=type or "StepError", non_retryable=non_retryable)
        self.step_name = step_name
        self.seq = seq
        self.attempts = attempts


class ChildWorkflowError(FailureError):
    """A child workflow failed, was cancelled or terminated, or could not start.

    The child's own failure (if any) is available as ``__cause__``.
    """

    def __init__(
        self,
        message: str,
        *,
        workflow_id: str,
        run_id: str,
        status: str,
        type: str | None = None,
        non_retryable: bool = False,
    ) -> None:
        super().__init__(message, type=type or "ChildWorkflowError", non_retryable=non_retryable)
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.status = status


class TimeoutFailure(FailureError):
    """A step attempt or a whole step exceeded its configured timeout."""


class CanceledFailure(FailureError):
    """The operation was canceled before it produced a result."""


class WorkflowFailedError(LoomworkError):
    """Raised by :meth:`WorkflowHandle.result` when the run failed."""

    def __init__(self, run_id: str, failure: FailureInfo) -> None:
        super().__init__(f"workflow run {run_id} failed: {failure.message}")
        self.run_id = run_id
        self.failure = failure


class UpdateFailedError(LoomworkError):
    """Raised by :meth:`WorkflowHandle.update` when the update handler failed."""

    def __init__(self, update_id: str, failure: FailureInfo) -> None:
        super().__init__(f"update {update_id} failed: {failure.message}")
        self.update_id = update_id
        self.failure = failure


class WorkflowCanceledError(LoomworkError):
    """Raised by :meth:`WorkflowHandle.result` when the run was canceled."""

    def __init__(self, run_id: str) -> None:
        super().__init__(f"workflow run {run_id} was canceled")
        self.run_id = run_id


class WorkflowTerminatedError(LoomworkError):
    """Raised by :meth:`WorkflowHandle.result` when the run was terminated."""

    def __init__(self, run_id: str, reason: str) -> None:
        super().__init__(f"workflow run {run_id} was terminated: {reason}")
        self.run_id = run_id
        self.reason = reason


class ContinueAsNew(BaseException):
    """Raised by :func:`loomwork.continue_as_new` to end the run and start a fresh one.

    It derives from :class:`BaseException` (like :class:`asyncio.CancelledError`) so
    that ``except Exception`` in workflow code does not swallow it by accident.
    """

    def __init__(self, workflow_name: str | None, args: list[object]) -> None:
        super().__init__("continue as new")
        self.workflow_name = workflow_name
        self.args = args


# --------------------------------------------------------------------------- engine / API


class NotFoundError(LoomworkError):
    """A workflow, run or registered definition does not exist."""


class WorkflowAlreadyStartedError(LoomworkError):
    """A running execution already uses the requested workflow id."""

    def __init__(self, workflow_id: str, run_id: str) -> None:
        super().__init__(f"workflow id {workflow_id!r} is already running as run {run_id}")
        self.workflow_id = workflow_id
        self.run_id = run_id


class WorkflowClosedError(LoomworkError):
    """An operation required a running execution but the run has already closed."""


class ScheduleExistsError(LoomworkError):
    """A schedule with this id already exists."""


class ScheduleConflictError(LoomworkError):
    """The schedule changed since it was read (optimistic concurrency check failed)."""


class RegistrationError(LoomworkError):
    """A workflow or step definition is invalid or registered twice."""


class NotInWorkflowError(LoomworkError):
    """A workflow-only API was called outside of workflow code."""


class LeaseLostError(LoomworkError):
    """A worker tried to commit work under a lease it no longer holds."""


class PayloadError(LoomworkError):
    """A value could not be encoded to, or decoded from, a payload."""


# --------------------------------------------------------------------------- tick errors


class WorkflowTickError(LoomworkError):
    """A tick could not be completed; it will be retried."""


class NondeterminismError(WorkflowTickError):
    """Workflow code produced commands that do not match its recorded history."""


class DeterminismViolation(WorkflowTickError):
    """Workflow code attempted a forbidden, non-deterministic operation (such as I/O)."""
