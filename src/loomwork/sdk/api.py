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
from typing import Any

from loomwork.errors import NotInWorkflowError
from loomwork.info import StepInfo, WorkflowInfo
from loomwork.runtime.context import current_instance, current_instance_or_none, in_workflow

__all__ = [
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
