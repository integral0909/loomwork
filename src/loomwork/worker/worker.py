"""Workers: the processes that execute workflow ticks, steps and timers.

A :class:`Worker` runs three kinds of loops against one store:

* a tick dispatcher running up to ``workflow_concurrency`` workflow ticks at once,
* a step dispatcher running up to ``step_concurrency`` step attempts at once, taking
  work only from the worker's ``task_queues`` and within its per-queue limits,
* one timer loop that fires due timers, and
* one outbox relay loop that delivers cross-run messages (child results, parent-close
  policies, signals between workflows). Both are optional per worker; one of each is
  needed per deployment, and running several is safe because both are atomic.

Idle dispatchers sleep until the store publishes a notification for their topic or the
poll interval elapses, whichever comes first.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field

from loomwork.clock import Clock, SystemClock
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.engine.config import EngineConfig
from loomwork.engine.steps import StepExecutor
from loomwork.engine.ticks import TickProcessor
from loomwork.ids import new_id
from loomwork.interceptors import StepInterceptor
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
from loomwork.versioning.builds import BuildVersioning
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
        step_interceptors: middleware wrapped around every step attempt, outermost
            first (see :mod:`loomwork.interceptors`).
        build_id: this worker's build, used to route ``PINNED`` runs (see
            :class:`~loomwork.options.VersioningPolicy` and ``docs/adr/
            0006-worker-build-versioning.md``). ``None`` (the default) means
            unversioned: this worker claims every tick and step it otherwise could,
            pinned or not, exactly as if this feature did not exist. Set it with
            ``loomwork worker --build-id`` or directly here; typically a short git SHA
            or an image tag.
        build_heartbeat_interval: seconds between presence heartbeats while
            ``build_id`` is set (ignored otherwise). Heartbeats are one cheap upsert
            each and are purely informational -- they never gate claiming -- so a long
            interval is fine; it only affects how quickly a dashboard notices a build
            has no workers left.
        report_build_drain: while ``build_id`` is set, also check on the same interval
            whether any pinned build has runs waiting but no live (or compatible)
            worker, and report each one found through
            :meth:`~loomwork.observability.telemetry.Telemetry.pinned_runs_waiting`.
            Redundant work if several workers all do it; harmless, since the check is a
            handful of indexed reads.
    """

    workflow_concurrency: int = 4
    step_concurrency: int = 16
    run_timers: bool = True
    run_outbox: bool = True
    run_schedules: bool = True
    shutdown_grace: float = 10.0
    task_queues: tuple[str, ...] | None = None
    queue_limits: Mapping[str, QueueLimit] = field(default_factory=dict)
    step_interceptors: Sequence[StepInterceptor] = ()
    build_id: str | None = None
    build_heartbeat_interval: float = 30.0
    report_build_drain: bool = True

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
        if self.build_heartbeat_interval <= 0:
            raise ValueError("build_heartbeat_interval must be positive")
        if self.build_id is not None and not self.build_id.strip():
            raise ValueError("build_id must not be blank")


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
            build_id=self.options.build_id,
        )
        self.steps = StepExecutor(
            store,
            registry,
            clock=self.clock,
            converter=self.converter,
            config=self.config,
            worker_id=self.worker_id,
            telemetry=self.telemetry,
            interceptors=self.options.step_interceptors,
            build_id=self.options.build_id,
        )
        self.schedules = ScheduleService(store, self.clock, self.telemetry)
        self.versioning = BuildVersioning(store, self.clock)
        self._gate = QueueGate(self.options.task_queues, self.options.queue_limits)
        self._local = Notifier()
        self._tasks: list[asyncio.Task[None]] = []
        self._inflight: set[asyncio.Task[object]] = set()
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
        if self.options.workflow_concurrency:
            self._spawn("ticks", self._tick_dispatcher())
        if self.options.step_concurrency:
            self._spawn("steps", self._step_dispatcher())
        if self.options.run_timers:
            self._spawn("timers", self._timer_loop())
        if self.options.run_outbox:
            self._spawn("outbox", self._outbox_loop())
        if self.options.run_schedules:
            self._spawn("schedules", self._schedule_loop())
        if self.options.build_id is not None:
            # Gated on build_id, not on report_build_drain: with no build id set
            # anywhere in the deployment, nothing can ever be pinned or orphaned, so an
            # unversioned worker does none of this -- no new store calls, no new loop.
            self._spawn("build-presence", self._build_presence_loop())
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
        """Stop claiming work, give in-flight work ``grace`` seconds, then cancel it.

        Cancelled step attempts are handed back for another worker to run at once; a
        cancelled tick leaves no trace and is retried as well.
        """
        if not self._tasks:
            return
        self._stopping.set()
        grace = self.options.shutdown_grace if grace is None else grace
        # Dispatchers and service loops notice the stop flag immediately.
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._inflight:
            _, pending = await asyncio.wait(set(self._inflight), timeout=grace)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        for task in self._tasks:
            if not task.cancelled() and task.exception() is not None:
                logger.error("worker loop crashed", exc_info=task.exception())
        self._tasks.clear()
        self.ticks.close()
        logger.info("worker %s stopped", self.worker_id)

    async def __aenter__(self) -> Worker:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    # ------------------------------------------------------------------ dispatch
    #
    # One dispatcher per kind of work claims jobs and runs each in its own task, bounded
    # by a semaphore. Idle workers therefore poll the store once per poll interval per
    # kind of work, rather than once per concurrency slot.

    async def _acquire(self, slots: asyncio.Semaphore) -> bool:
        """Wait for a free slot; ``False`` if the worker is stopping instead."""
        if self._stopping.is_set():
            return False
        acquire = asyncio.ensure_future(slots.acquire())
        stop = asyncio.ensure_future(self._stopping.wait())
        await asyncio.wait({acquire, stop}, return_when=asyncio.FIRST_COMPLETED)
        stop.cancel()
        if not acquire.done():
            acquire.cancel()
        results = await asyncio.gather(acquire, stop, return_exceptions=True)
        got_slot = results[0] is True
        if got_slot and self._stopping.is_set():
            slots.release()
            return False
        return got_slot

    def _launch(
        self,
        work: Awaitable[object],
        slots: asyncio.Semaphore,
        name: str,
        on_done: Callable[[], None] | None = None,
    ) -> None:
        task = asyncio.ensure_future(work)
        task.set_name(f"loomwork-{name}")
        self._inflight.add(task)
        self._busy += 1

        def finished(done: asyncio.Future[object]) -> None:
            self._inflight.discard(task)
            self._busy -= 1
            slots.release()
            if on_done is not None:
                on_done()
            if not done.cancelled() and done.exception() is not None:
                self.telemetry.worker_error(name)
                logger.error("error in %s", name, exc_info=done.exception())

        task.add_done_callback(finished)

    async def _tick_dispatcher(self) -> None:
        slots = asyncio.Semaphore(self.options.workflow_concurrency)
        notifier = self.store.notifier
        while await self._acquire(slots):
            token = notifier.token(TOPIC_TICKS)
            try:
                record = await self.ticks.claim()
            except Exception:
                slots.release()
                logger.exception("error while claiming workflow ticks")
                self.telemetry.worker_error(TOPIC_TICKS)
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            if record is None:
                slots.release()
                await self._idle(notifier.wait(TOPIC_TICKS, token, self.config.poll_interval))
                continue
            self._launch(self.ticks.process(record), slots, "tick")

    async def _step_dispatcher(self) -> None:
        slots = asyncio.Semaphore(self.options.step_concurrency)
        notifier = self.store.notifier
        poll = self.config.poll_interval
        while await self._acquire(slots):
            token = notifier.token(TOPIC_STEPS)
            capacity = self._local.token(_CAPACITY)
            queues = self._gate.open_queues()
            if queues is not None and not queues:
                # Every queue is at its rate or concurrency limit.
                slots.release()
                wait = self._gate.seconds_until_open()
                timeout = poll if wait is None else min(wait, poll)
                await self._idle(self._local.wait(_CAPACITY, capacity, timeout))
                continue
            try:
                job = await self.steps.claim(queues)
                next_at = (
                    None
                    if job is not None
                    else await self.store.next_step_ready_at(self.registry.step_names, queues)
                )
            except Exception:
                slots.release()
                logger.exception("error while claiming steps")
                self.telemetry.worker_error("steps")
                await self._idle(self._sleep(_ERROR_BACKOFF_SECONDS))
                continue
            if job is not None:
                queue = job.queue
                self._gate.acquire(queue)

                def done(queue: str = queue) -> None:
                    self._gate.release(queue)
                    self._local.publish(_CAPACITY)

                self._launch(self.steps.execute(job), slots, "step", done)
                continue
            slots.release()
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

    async def _build_presence_loop(self) -> None:
        """Heartbeat this worker's build id (if any) and, less importantly, warn about
        pinned runs stuck on a build with nobody left to process them.

        Both checks are cheap (one upsert, a handful of indexed reads) and purely
        informational: nothing here ever affects claiming. Several workers all doing
        this is harmless duplication, not a correctness concern, so there is no
        election or single-worker restriction.
        """
        interval = self.options.build_heartbeat_interval
        ttl_seconds = interval * 3
        while not self._stopping.is_set():
            try:
                if self.options.build_id is not None:
                    await self.versioning.heartbeat(
                        self.worker_id, self.options.build_id, ttl_seconds
                    )
                if self.options.report_build_drain:
                    for summary in await self.versioning.builds_summary():
                        if summary.orphaned:
                            self.telemetry.pinned_runs_waiting(
                                summary.build_id, summary.pinned_running_runs
                            )
            except Exception:
                logger.exception("error while reporting worker build presence")
                self.telemetry.worker_error("build-presence")
            await self._idle(self._sleep(interval))

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
