"""Locating the workflow instance that is currently executing.

Workflow code runs on a :class:`DeterministicEventLoop` whose ``owner`` is the
:class:`WorkflowInstance`. Looking the instance up through the running loop (rather than a
context variable) is robust against user code that copies or replaces contexts.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from loomwork.errors import NotInWorkflowError
from loomwork.runtime.loop import DeterministicEventLoop

if TYPE_CHECKING:
    from loomwork.runtime.instance import WorkflowInstance


def current_instance_or_none() -> WorkflowInstance | None:
    loop = asyncio.events._get_running_loop()
    if isinstance(loop, DeterministicEventLoop):
        owner: WorkflowInstance | None = loop.owner
        return owner
    return None


def current_instance() -> WorkflowInstance:
    instance = current_instance_or_none()
    if instance is None:
        raise NotInWorkflowError("this function can only be called from workflow code")
    return instance


def in_workflow() -> bool:
    """Whether the caller is running inside workflow code."""
    return current_instance_or_none() is not None
