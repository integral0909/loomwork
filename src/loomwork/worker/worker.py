"""Workers: the processes that execute workflow ticks, steps and timers.

A :class:`Worker` runs three kinds of loops against one store:

* ``workflow_concurrency`` tick loops, each executing one workflow tick at a time,
* ``step_concurrency`` step loops, each executing one step attempt at a time,
* one timer loop that fires due timers (optional; only one is needed per deployment,
  although running several is safe because firing is atomic).

Idle loops sleep until the store publishes a notification for their topic or the poll
interval elapses, whichever comes first.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from loomwork.clock import Clock, SystemClock
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.engine.config import EngineConfig
from loomwork.engine.steps import StepExecutor
from loomwork.engine.ticks import TickProcessor
from loomwork.ids import new_id
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.notify import TOPIC_STEPS, TOPIC_TICKS, TOPIC_TIMERS

logger = logging.getLogger("loomwork.worker")

_ERROR_BACKOFF_SECONDS = 1.0


def default_worker_id() -> str:
    """``hostname:pid:random`` — unique per process, readable in operator tooling."""
    return f"{socket.gethostname()}:{os.getpid()}:{new_id()[-6:]}"


@dataclass(frozen=True, slots=True)
class WorkerOptions:
    workflow_concurrency: int = 4
    step_concurrency: int = 16
    run_timers: bool = True
    shutdown_grace: float = 10.0

    def __post_init__(self) -> None:
        if self.workflow_concurrency < 0 or self.step_concurrency < 0:
            raise ValueError("concurrency must be >= 0")
        if self.shutdown_grace < 0:
            raise ValueError("shutdown_grace must be >= 0")


class Worker:
    """Executes workflows and steps registered in ``registry``."""

    def __init__(
        self,
        store: Store,
        registry: Registry,
        *,
        clock: Clock | None = None,
        converter: DataConverter | None = None,
        config: EngineConfig | None = None,
        options: WorkerOptions | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.store = store
        self.registry = registry
        self.clock = clock or SystemClock()
        self.converter = converter or default_converter
        self.config = config or EngineConfig()
        self.options = options or WorkerOptions()
        self.worker_id = worker_id or default_worker_id()
        self.ticks = TickProcessor(
            store,
            registry,
            clock=self.clock,
            converter=self.converter,
            config=self.config,
            worker_id=self.worker_id,
        )
        self.steps = StepExecutor(
            store,
            registry,
            clock=self.clock,
            converter=self.converter,
            config=self.config,
            worker_id=self.worker_id,
        )
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()
        self._busy = 0

    # ------------------------------------------------------------------ lifecycle

    @property
    def running(self) -> bool:
        return bool(self._tasks) and not self._stopping.is_set()

    async def start(self) -> None:
        if self._tasks:
            raise RuntimeError("worker already started")
        self._stopping.clear()
        for index in range(self.options.workflow_concurrency):
            self._spawn(f"ticks-{index}", self._poll_loop(TOPIC_TICKS, self.ticks.run_once))
        for index in range(self.options.step_concurrency):
            self._spawn(f"steps-{index}", self._poll_loop(TOPIC_STEPS, self.steps.run_once))
        if self.options.run_timers:
            self._spawn("timers", self._timer_loop())
        logger.info(
            "worker %s started (%d workflows, %d steps registered)",
            self.worker_id,
            len(self.registry.workflow_names),
            len(self.registry.step_names),
        )

    def _spawn(self, name: str, coro: Awaitable[None]) -> None:
        task = asyncio.ensure_future(coro)
        task.set_name(f"loomwork-{name}")
        self._tasks.append(task)

    async def stop(self, grace: float | None = None) -> None:
        """Stop polling, give in-flight work ``grace`` seconds, then cancel it.

        Cancelled step attempts are retried elsewhere once their lease expires; a
        cancelled tick leaves no trace and is retried as well.
        """
        if not self._tasks:
            return
        self._stopping.set()
        grace = self.options.shutdown_grace if grace is None else grace
        _, pending = await asyncio.wait(self._tasks, timeout=grace)
        for task in pending:
            task.cancel()
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error("worker loop crashed", exc_info=result)
        self._tasks.clear()
        self.ticks.close()
        logger.info("worker %s stopped", self.worker_id)

    async def __aenter__(self) -> Worker:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    # ------------------------------------------------------------------ loops

    async def _poll_loop(self, topic: str, run_once: Callable[[], Awaitable[bool]]) -> None:
        notifier = self.store.notifier
        while not self._stopping.is_set():
            token = notifier.token(topic)
            try:
                self._busy += 1
                try:
                    did_work = await run_once()
                finally:
                    self._busy -= 1
            except Exception:
                logger.exception("error while polling for %s", topic)
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            if not did_work:
                await self._idle(notifier.wait(topic, token, self.config.poll_interval))

    async def _timer_loop(self) -> None:
        notifier = self.store.notifier
        while not self._stopping.is_set():
            token = notifier.token(TOPIC_TIMERS)
            try:
                fired = await self.store.fire_due_timers(
                    self.clock.now(), self.config.timer_batch_size
                )
                if fired >= self.config.timer_batch_size:
                    continue
                next_at = await self.store.next_timer_at()
            except Exception:
                logger.exception("error while firing timers")
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            waits: list[Awaitable[object]] = [
                notifier.wait(TOPIC_TIMERS, token, self.config.poll_interval)
            ]
            if next_at is not None:
                waits.append(self.clock.sleep_until(next_at))
            await self._idle(*waits)

    @staticmethod
    async def _sleep(seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def _idle(self, *waits: Awaitable[object]) -> None:
        """Wait for the first of ``waits`` or for shutdown."""
        tasks = [asyncio.ensure_future(w) for w in waits]
        stop = asyncio.ensure_future(self._stopping.wait())
        try:
            await asyncio.wait([*tasks, stop], return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (*tasks, stop):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, stop, return_exceptions=True)

    # ------------------------------------------------------------------ introspection

    @property
    def busy(self) -> int:
        """Number of loops currently executing a tick or a step attempt."""
        return self._busy
