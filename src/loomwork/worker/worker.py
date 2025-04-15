"""Workers: the processes that execute workflow ticks, steps and timers.

A :class:`Worker` runs three kinds of loops against one store:

* ``workflow_concurrency`` tick loops, each executing one workflow tick at a time,
* ``step_concurrency`` step loops, each executing one step attempt at a time, taking
  work only from the worker's ``task_queues`` and within its per-queue limits,
* one timer loop that fires due timers, and
* one outbox relay loop that delivers cross-run messages (child results, parent-close
  policies, signals between workflows). Both are optional per worker; one of each is
  needed per deployment, and running several is safe because both are atomic.

Idle loops sleep until the store publishes a notification for their topic or the poll
interval elapses, whichever comes first.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field

from loomwork.clock import Clock, SystemClock
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.engine.config import EngineConfig
from loomwork.engine.steps import StepExecutor
from loomwork.engine.ticks import TickProcessor
from loomwork.ids import new_id
from loomwork.observability.telemetry import NoopTelemetry, Telemetry
from loomwork.options import validate_queue
from loomwork.schedules.service import ScheduleService
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.notify import (
    TOPIC_OUTBOX,
    TOPIC_SCHEDULES,
    TOPIC_STEPS,
    TOPIC_TICKS,
    TOPIC_TIMERS,
    Notifier,
)
from loomwork.worker.limits import QueueGate, QueueLimit

logger = logging.getLogger("loomwork.worker")

_ERROR_BACKOFF_SECONDS = 1.0


def default_worker_id() -> str:
    """``hostname:pid:random`` — unique per process, readable in operator tooling."""
    return f"{socket.gethostname()}:{os.getpid()}:{new_id()[-6:]}"


_CAPACITY = "capacity"


@dataclass(frozen=True, slots=True)
class WorkerOptions:
    """How a worker participates in the deployment.

    Attributes:
        workflow_concurrency: concurrent workflow ticks.
        step_concurrency: concurrent step attempts across all queues.
        run_timers: whether this worker fires due timers.
        run_outbox: whether this worker relays outbox messages between runs.
        run_schedules: whether this worker evaluates schedules and starts their runs.
        shutdown_grace: seconds in-flight work gets to finish on :meth:`Worker.stop`.
        task_queues: queues whose steps this worker executes; ``None`` means every
            queue. Workers that hold special credentials or hardware subscribe to their
            own queue.
        queue_limits: per-queue rate (attempts/second) and concurrency limits. Every
            limited queue must be listed in ``task_queues``.
    """

    workflow_concurrency: int = 4
    step_concurrency: int = 16
    run_timers: bool = True
    run_outbox: bool = True
    run_schedules: bool = True
    shutdown_grace: float = 10.0
    task_queues: tuple[str, ...] | None = None
    queue_limits: Mapping[str, QueueLimit] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.workflow_concurrency < 0 or self.step_concurrency < 0:
            raise ValueError("concurrency must be >= 0")
        if self.shutdown_grace < 0:
            raise ValueError("shutdown_grace must be >= 0")
        if self.task_queues is not None:
            if not self.task_queues:
                raise ValueError("task_queues must not be empty (use None for all queues)")
            for queue in self.task_queues:
                validate_queue(queue)
        if self.queue_limits and self.task_queues is None:
            raise ValueError("queue_limits require an explicit task_queues list")


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
        telemetry: Telemetry | None = None,
    ) -> None:
        self.telemetry: Telemetry = telemetry or NoopTelemetry()
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
            telemetry=self.telemetry,
        )
        self.steps = StepExecutor(
            store,
            registry,
            clock=self.clock,
            converter=self.converter,
            config=self.config,
            worker_id=self.worker_id,
            telemetry=self.telemetry,
        )
        self.schedules = ScheduleService(store, self.clock, self.telemetry)
        self._gate = QueueGate(self.options.task_queues, self.options.queue_limits)
        self._local = Notifier()
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
            self._spawn(f"steps-{index}", self._step_loop())
        if self.options.run_timers:
            self._spawn("timers", self._timer_loop())
        if self.options.run_outbox:
            self._spawn("outbox", self._outbox_loop())
        if self.options.run_schedules:
            self._spawn("schedules", self._schedule_loop())
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
                self.telemetry.worker_error(topic)
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            if not did_work:
                await self._idle(notifier.wait(topic, token, self.config.poll_interval))

    async def _step_loop(self) -> None:
        notifier = self.store.notifier
        poll = self.config.poll_interval
        while not self._stopping.is_set():
            token = notifier.token(TOPIC_STEPS)
            capacity = self._local.token(_CAPACITY)
            queues = self._gate.open_queues()
            if queues is not None and not queues:
                # Every queue is at its rate or concurrency limit.
                wait = self._gate.seconds_until_open()
                timeout = poll if wait is None else min(wait, poll)
                await self._idle(self._local.wait(_CAPACITY, capacity, timeout))
                continue
            try:
                self._busy += 1
                try:
                    job = await self.steps.claim(queues)
                    if job is not None:
                        self._gate.acquire(job.queue)
                        try:
                            await self.steps.execute(job)
                        finally:
                            self._gate.release(job.queue)
                            self._local.publish(_CAPACITY)
                finally:
                    self._busy -= 1
                if job is not None:
                    continue
                next_at = await self.store.next_step_ready_at(self.registry.step_names, queues)
            except Exception:
                logger.exception("error while executing steps")
                self.telemetry.worker_error("steps")
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            waits: list[Awaitable[object]] = [notifier.wait(TOPIC_STEPS, token, poll)]
            if next_at is not None:
                # A retry backoff or delayed job becomes ready: wake exactly then.
                waits.append(self.clock.sleep_until(next_at))
            await self._idle(*waits)

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
                self.telemetry.worker_error("timers")
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            waits: list[Awaitable[object]] = [
                notifier.wait(TOPIC_TIMERS, token, self.config.poll_interval)
            ]
            if next_at is not None:
                waits.append(self.clock.sleep_until(next_at))
            await self._idle(*waits)

    async def _outbox_loop(self) -> None:
        notifier = self.store.notifier
        batch = self.config.timer_batch_size
        while not self._stopping.is_set():
            token = notifier.token(TOPIC_OUTBOX)
            try:
                relayed = await self.store.relay_outbox(self.clock.now(), batch)
                self.telemetry.outbox_relayed(relayed)
            except Exception:
                logger.exception("error while relaying outbox messages")
                self.telemetry.worker_error("outbox")
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            if relayed >= batch:
                continue
            await self._idle(notifier.wait(TOPIC_OUTBOX, token, self.config.poll_interval))

    async def _schedule_loop(self) -> None:
        notifier = self.store.notifier
        while not self._stopping.is_set():
            token = notifier.token(TOPIC_SCHEDULES)
            try:
                await self.schedules.run_once()
                next_at = await self.store.next_schedule_at()
            except Exception:
                logger.exception("error while evaluating schedules")
                self.telemetry.worker_error("schedules")
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            waits: list[Awaitable[object]] = [
                notifier.wait(TOPIC_SCHEDULES, token, self.config.poll_interval)
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
