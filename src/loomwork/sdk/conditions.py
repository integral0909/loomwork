"""``wait_condition``: block workflow code until its own state satisfies a predicate.

Conditions are re-evaluated when something that can change workflow state from outside
finishes: a signal handler of a class-based workflow, or any update handler. This keeps
them free of polling (which would need timers and grow the history) while staying
deterministic: the re-evaluation points are themselves replayed events.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import weakref
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from loomwork.runtime.context import current_instance


class _Conditions:
    """Waiters of :func:`wait_condition` in one workflow instance."""

    def __init__(self) -> None:
        self.waiters: list[asyncio.Future[None]] = []

    def notify(self) -> None:
        waiters, self.waiters = self.waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def wait(self) -> None:
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiters.append(waiter)
        await waiter


_conditions: weakref.WeakKeyDictionary[object, _Conditions] = weakref.WeakKeyDictionary()


def conditions_of_current() -> _Conditions:
    instance = current_instance()
    conditions = _conditions.get(instance)
    if conditions is None:
        conditions = _conditions[instance] = _Conditions()
    return conditions


def notify_conditions() -> None:
    """Re-evaluate every pending :func:`wait_condition` of the running workflow.

    Signal and update handlers call this automatically; call it yourself after changing
    state from other workflow code that a condition depends on. *Workflow-only.*
    """
    conditions_of_current().notify()


async def wait_condition(
    predicate: Callable[[], bool], *, timeout: timedelta | float | None = None
) -> None:
    """Wait until ``predicate()`` is true. *Workflow-only.*

    The predicate is checked immediately and again whenever a signal or update handler
    finishes (or :func:`notify_conditions` is called). It must only read workflow state.

    Raises:
        TimeoutError: ``timeout`` (a durable timer) elapsed first.
    """
    conditions = conditions_of_current()
    seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout

    async def wait() -> None:
        while not predicate():
            await conditions.wait()

    if seconds is None:
        await wait()
        return
    async with asyncio.timeout(seconds):
        await wait()


def notifying(handler: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap an update handler so conditions are re-checked after it runs."""
    if inspect.iscoroutinefunction(handler):

        @functools.wraps(handler)
        async def async_wrapper(*args: Any) -> Any:
            try:
                return await handler(*args)
            finally:
                conditions_of_current().notify()

        return async_wrapper

    @functools.wraps(handler)
    def wrapper(*args: Any) -> Any:
        try:
            return handler(*args)
        finally:
            conditions_of_current().notify()

    return wrapper


__all__ = ["conditions_of_current", "notify_conditions", "notifying", "wait_condition"]
