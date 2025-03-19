"""Functions available to workflow and step code.

Everything here that is marked *workflow-only* derives its result from the recorded
history, so it returns the same value when a run is replayed.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import random as _random
import uuid as _uuid
from collections.abc import MutableMapping
from datetime import datetime, timedelta
from typing import Any, NoReturn, Protocol

from loomwork.errors import ContinueAsNew, NotInWorkflowError
from loomwork.info import StepInfo, WorkflowInfo
from loomwork.runtime.context import current_instance, current_instance_or_none, in_workflow

__all__ = [
    "deprecate_patch",
    "patched",
    "continue_as_new",
    "continue_as_new_suggested",
    "history_length",
    "heartbeat",
    "heartbeat_details",
    "in_workflow",
    "info",
    "is_replaying",
    "logger",
    "now",
    "random",
    "sleep",
    "sleep_until",
    "step_info",
    "uuid4",
]

# --------------------------------------------------------------------------- workflow-only


def now() -> datetime:
    """Current workflow time (UTC). *Workflow-only.*

    This is the time at which the current activation started, not the wall clock. It
    only moves between activations, and never backwards.
    """
    return current_instance().now


def info() -> WorkflowInfo:
    """Identity of the running workflow. *Workflow-only.*"""
    return current_instance().info


def random() -> _random.Random:
    """A :class:`random.Random` seeded per run. *Workflow-only.*

    Use this instead of the :mod:`random` module inside workflows.
    """
    return current_instance().random


def uuid4() -> _uuid.UUID:
    """A random-looking UUID that is identical on replay. *Workflow-only.*"""
    return current_instance().uuid4()


def is_replaying() -> bool:
    """Whether the current activation is re-executing recorded history. *Workflow-only.*"""
    return current_instance().replaying


def continue_as_new(*args: Any, workflow: Any = None) -> NoReturn:
    """End this run and start a fresh run of the same workflow id. *Workflow-only.*

    Long-lived workflows (an entity that lives for months, a loop over millions of items)
    call this periodically to keep their history small. The new run starts with
    ``args`` and an empty history; pass whatever state it needs to resume. ``workflow``
    (a definition) switches to a different workflow, for example a newer version.
    """
    current_instance()
    name = getattr(workflow, "name", None) if workflow is not None else None
    raise ContinueAsNew(name, list(args))


def patched(patch_id: str) -> bool:
    """Branch between old and new workflow logic safely. *Workflow-only.*

    Returns ``True`` when the run reaches this point under code that knows the patch
    (every new run, and running ones that get here after the deploy), and ``False``
    while replaying history recorded before the patch existed::

        if lw.patched("send-sms-receipt"):
            await send_sms(order)        # new behaviour
        else:
            await send_email(order)      # what old runs already did

    Once no run that predates the patch is left, replace the call with
    :func:`deprecate_patch` and delete the old branch; later remove that call too.
    """
    return current_instance().patched(patch_id)


def deprecate_patch(patch_id: str) -> None:
    """Mark a patch whose old branch has been deleted. *Workflow-only.*

    Keeps runs that recorded the patch replaying correctly while the new code no longer
    branches.
    """
    current_instance().deprecate_patch(patch_id)


def history_length() -> int:
    """Approximate number of events in this run's history. *Workflow-only.*"""
    return current_instance().history_length


def continue_as_new_suggested() -> bool:
    """Whether the history has grown past the engine's recommended size. *Workflow-only.*"""
    instance = current_instance()
    return instance.history_length >= instance.continue_as_new_suggested_at


async def sleep(duration: timedelta | float) -> None:
    """Durably sleep for ``duration`` (seconds or ``timedelta``). *Workflow-only.*

    Equivalent to ``asyncio.sleep`` inside a workflow; provided for readability and to
    reject accidental use outside workflows, where a durable sleep is impossible.
    """
    current_instance()
    seconds = duration.total_seconds() if isinstance(duration, timedelta) else float(duration)
    if seconds < 0:
        raise ValueError("sleep duration must not be negative")
    await asyncio.sleep(seconds)


async def sleep_until(deadline: datetime) -> None:
    """Durably sleep until ``deadline``. *Workflow-only.*"""
    if deadline.tzinfo is None:
        raise ValueError("deadline must be timezone-aware")
    delta = (deadline - current_instance().now).total_seconds()
    if delta > 0:
        await asyncio.sleep(delta)


# --------------------------------------------------------------------------- steps

_step_info: contextvars.ContextVar[StepInfo | None] = contextvars.ContextVar(
    "loomwork_step_info", default=None
)


def step_info() -> StepInfo:
    """Identity of the running step attempt. *Step-only.*"""
    value = _step_info.get()
    if value is None:
        raise NotInWorkflowError("step_info() can only be called while a step is executing")
    return value


def _set_step_info(value: StepInfo | None) -> contextvars.Token[StepInfo | None]:
    return _step_info.set(value)


def _reset_step_info(token: contextvars.Token[StepInfo | None]) -> None:
    _step_info.reset(token)


class AttemptContext(Protocol):
    """Implemented by the step executor for the attempt currently running."""

    async def heartbeat(self, details: Any) -> None: ...

    def previous_details(self, hint: Any) -> Any: ...


_attempt: contextvars.ContextVar[AttemptContext | None] = contextvars.ContextVar(
    "loomwork_attempt", default=None
)


def _set_attempt(value: AttemptContext | None) -> contextvars.Token[AttemptContext | None]:
    return _attempt.set(value)


def _reset_attempt(token: contextvars.Token[AttemptContext | None]) -> None:
    _attempt.reset(token)


def _require_attempt() -> AttemptContext:
    attempt = _attempt.get()
    if attempt is None:
        raise NotInWorkflowError("heartbeats can only be sent while a step is executing")
    return attempt


async def heartbeat(details: Any = None) -> None:
    """Report that a long-running step is alive. *Step-only.*

    Heartbeats renew the attempt's lease, so a step with ``heartbeat_timeout`` is
    retried soon after its worker dies instead of after its full ``timeout``.
    ``details`` (any encodable value) is stored and handed to the next attempt through
    :func:`heartbeat_details`, which lets a retry resume from the last checkpoint.

    Calls are throttled, so it is fine to call this in a tight loop. If the workflow no
    longer needs the step's result, the attempt is cancelled: the step sees
    :class:`asyncio.CancelledError` at its next ``await``.
    """
    await _require_attempt().heartbeat(details)


def heartbeat_details(hint: Any = Any) -> Any:
    """Details from the last heartbeat of a previous attempt, or ``None``. *Step-only.*"""
    return _require_attempt().previous_details(hint)


# --------------------------------------------------------------------------- logging


class _ContextLogger(logging.LoggerAdapter[logging.Logger]):
    """Logger that is quiet during replay and tags records with run identity.

    Inside workflow code, records emitted while replaying are dropped, so each log line
    appears once even though the code runs many times. Inside steps, records carry the
    step's identity.
    """

    def isEnabledFor(self, level: int) -> bool:  # noqa: N802 - logging API name
        instance = current_instance_or_none()
        if instance is not None and instance.replaying:
            return False
        return self.logger.isEnabledFor(level)

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        extra: dict[str, Any] = dict(kwargs.get("extra") or {})
        instance = current_instance_or_none()
        if instance is not None:
            extra.setdefault("workflow_id", instance.info.workflow_id)
            extra.setdefault("run_id", instance.info.run_id)
            extra.setdefault("workflow_name", instance.info.workflow_name)
        else:
            current_step = _step_info.get()
            if current_step is not None:
                extra.setdefault("workflow_id", current_step.workflow_id)
                extra.setdefault("run_id", current_step.run_id)
                extra.setdefault("step_name", current_step.step_name)
                extra.setdefault("step_attempt", current_step.attempt)
        kwargs["extra"] = extra
        return msg, kwargs


logger = _ContextLogger(logging.getLogger("loomwork.user"), {})
