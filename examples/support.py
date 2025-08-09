"""Small helpers shared by the example apps' ``main.py`` scripts.

Not part of the public Loomwork API surface -- each example could inline this, but the
pattern (drive a :class:`~loomwork.ManualClock` forward until some condition holds, so
a demo that models hours or days of real time finishes in well under a second) recurs
often enough across the examples to be worth sharing once.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from loomwork import ManualClock, WorkflowHandle
from loomwork.errors import NotFoundError

#: An engine/worker poll interval short enough that examples settle quickly, but long
#: enough not to busy-loop. Every example's ``main.py`` uses this for its EngineConfig.
DEMO_POLL_INTERVAL = 0.02


async def advance_until(
    clock: ManualClock,
    predicate: Callable[[], Awaitable[bool] | bool],
    *,
    step: timedelta | float = timedelta(seconds=1),
    timeout: float = 10.0,
) -> None:
    """Advance ``clock`` by ``step`` until ``predicate`` holds, or raise on ``timeout``.

    Mirrors the passage of real time a deployment would experience while a durable
    timer, a step retry backoff or a schedule's next fire time is pending, but collapses
    it into however many polling iterations ``predicate`` needs to flip -- so a demo of
    a multi-day timeout runs in a fraction of a second instead of actually waiting days.
    """
    delta = step if isinstance(step, timedelta) else timedelta(seconds=step)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if loop.time() > deadline:
            raise TimeoutError("condition was not met before the timeout")
        await clock.advance(delta)
        # Give worker loops a real (if tiny) slice of wall-clock time to react to the
        # new clock value -- ticks, steps and schedule evaluations all happen as
        # ordinary asyncio tasks, and a bare `sleep(0)` yields only once, which is not
        # always enough for a whole chain of them (timer fires -> tick -> step -> tick)
        # to settle when several workflows are in flight at once.
        await asyncio.sleep(0.01)


async def wait_for_query(
    clock: ManualClock,
    handle: WorkflowHandle[Any],
    name: str,
    predicate: Callable[[Any], bool],
    *args: Any,
    step: timedelta | float = timedelta(seconds=1),
    timeout: float = 10.0,
) -> Any:
    """Advance ``clock`` until ``handle.query(name, *args)`` satisfies ``predicate``.

    Tolerates the query racing the workflow's very first tick -- which is what
    registers its query handlers -- by treating :class:`~loomwork.errors.NotFoundError`
    as "not ready yet" rather than letting it propagate.
    """

    async def ready() -> bool:
        try:
            value = await handle.query(name, *args)
        except NotFoundError:
            return False
        return predicate(value)

    await advance_until(clock, ready, step=step, timeout=timeout)
    return await handle.query(name, *args)


def rule(title: str) -> str:
    """A short banner line for a demo's console narrative."""
    return f"\n--- {title} ---"
