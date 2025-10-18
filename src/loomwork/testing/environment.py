"""An in-memory, time-skipping test harness for Loomwork workflows.

A :class:`WorkflowEnvironment` runs a :class:`~loomwork.Loomwork` app against an
in-memory store and a :class:`~loomwork.clock.ManualClock`, and continuously watches
for the moment every run it hosts is blocked purely on a durable timer, a step retry
backoff or a schedule -- nothing is runnable at the current instant. At that moment it
jumps the clock straight to the next such deadline, so a workflow that sleeps for
thirty days completes in a handful of milliseconds of wall-clock time, without the test
ever having to know how long the workflow waits or hand-roll a polling loop around a
:class:`~loomwork.clock.ManualClock` (the pattern this replaces; see
``tests/helpers.eventually`` in Loomwork's own test suite).

Auto time-skipping only ever advances the clock when nothing else can make progress: a
worker mid-tick or mid-step, a step ready to claim, a timer already due, a schedule
already due, or an outbox message waiting to be relayed all hold the clock where it is.
A slow step that genuinely takes real wall-clock time (a test double calling
``asyncio.sleep`` directly, for example) is therefore never cut short or skipped over --
see ``test_time_skipping_never_skips_runnable_work`` in this package's test suite for a
regression test of exactly that.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar, overload

from loomwork.app import Loomwork
from loomwork.client.client import Client
from loomwork.client.handle import WorkflowHandle
from loomwork.clock import ManualClock
from loomwork.codec.converter import DataConverter
from loomwork.engine.config import EngineConfig
from loomwork.retry import RetryPolicy
from loomwork.sdk.definitions import (
    Registry,
    StepDefinition,
    WorkflowDefinition,
    WorkflowLike,
)
from loomwork.sdk.messaging import Channel
from loomwork.store.memory import MemoryStore
from loomwork.store.notify import (
    TOPIC_OUTBOX,
    TOPIC_SCHEDULES,
    TOPIC_STEPS,
    TOPIC_TICKS,
    TOPIC_TIMERS,
)
from loomwork.store.records import ExecutionRecord, ExecutionStatus
from loomwork.testing.assertions import HistoryAssertion, HistorySource, HistoryView, events_of
from loomwork.testing.golden import assert_replays as _assert_replays
from loomwork.testing.golden import record_golden as _record_golden
from loomwork.testing.golden import replay_check as _replay_check
from loomwork.testing.mocks import UNSET, StepMock, install_step, install_step_mock
from loomwork.worker.worker import Worker, WorkerOptions

R = TypeVar("R")

logger = logging.getLogger("loomwork.testing")

_NO_HINT: Any = inspect.Parameter.empty

#: An :class:`~loomwork.engine.config.EngineConfig` tuned for tests: short poll
#: intervals and backoffs so that whatever real wall-clock time a test does spend
#: (waiting for an actual notification, or between auto-skip checks) is milliseconds,
#: not seconds. Workflow-visible timing is unaffected -- it is driven entirely by the
#: :class:`~loomwork.clock.ManualClock`, never by this configuration.
TESTING_CONFIG = EngineConfig(
    poll_interval=0.01,
    tick_retry=RetryPolicy(initial_interval=0.01, max_interval=0.05, max_attempts=0, jitter=0),
    default_step_retry=RetryPolicy(initial_interval=0.01, max_interval=0.05, jitter=0),
    step_lease_grace=timedelta(seconds=5),
)

_IDLE_TOPICS = (TOPIC_TICKS, TOPIC_STEPS, TOPIC_TIMERS, TOPIC_OUTBOX, TOPIC_SCHEDULES)
_IDLE_FALLBACK_SECONDS = 0.02
#: How often a busy worker is re-checked; short enough not to delay tests noticeably.
_BUSY_POLL_SECONDS = 0.001
#: Scheduler turns given to in-flight work before a "nothing is actionable" reading is
#: trusted enough to jump the clock. Generous relative to its cost (each is a bare
#: ``asyncio.sleep(0)``): the store itself never leaves a "something is about to
#: happen" state without also updating the counters this module reads, so what these
#: yields protect against is purely tasks that were *woken* but have not yet run.
_STABILITY_YIELDS = 5


@dataclass(frozen=True, slots=True)
class _Snapshot:
    """A point-in-time reading of everything auto time-skipping needs to decide."""

    busy: int
    tick_at: datetime | None
    step_at: datetime | None
    timer_at: datetime | None
    schedule_at: datetime | None
    outbox: int

    def actionable(self, now: datetime) -> bool:
        """Whether some worker loop can make progress *right now*, without the clock
        moving -- in which case skipping ahead would race real, runnable work."""
        if self.busy or self.outbox:
            return True
        return any(
            deadline is not None and deadline <= now
            for deadline in (self.tick_at, self.step_at, self.timer_at, self.schedule_at)
        )

    def next_deadline(self, now: datetime) -> datetime | None:
        """The nearest future time worth jumping to, if any."""
        future = [
            deadline
            for deadline in (self.tick_at, self.step_at, self.timer_at, self.schedule_at)
            if deadline is not None and deadline > now
        ]
        return min(future, default=None)


class WorkflowEnvironment:
    """A self-contained, fast, deterministic place to run workflows in tests.

    .. code-block:: python

        async with WorkflowEnvironment(workflows=[process_order], steps=[charge_card]) as env:
            env.mock_step(charge_card, return_value="ch_123")
            receipt = await env.execute(process_order, order)
            assert receipt.charge_id == "ch_123"

    Args:
        workflows, steps: definitions to run, exactly as passed to :class:`~loomwork.Loomwork`.
        start_time: the :class:`~loomwork.clock.ManualClock`'s initial time; defaults to
            its own default (midnight UTC on 2024-01-01).
        auto_time_skipping: when true (the default), idle time is skipped automatically
            (see the module docstring). Set to ``False`` to drive the clock entirely
            with :meth:`sleep`, for tests that care about the exact sequence of real
            wake-ups a production deployment would see.
        config: overrides :data:`TESTING_CONFIG`.
        worker_options: overrides the default :class:`~loomwork.worker.WorkerOptions`.
        converter: overrides the default data converter.
    """

    def __init__(
        self,
        *,
        workflows: Iterable[WorkflowLike] = (),
        steps: Iterable[StepDefinition[..., Any]] = (),
        start_time: datetime | None = None,
        auto_time_skipping: bool = True,
        config: EngineConfig | None = None,
        worker_options: WorkerOptions | None = None,
        converter: DataConverter | None = None,
    ) -> None:
        self.clock = ManualClock(start_time)
        self.store = MemoryStore()
        self.app = Loomwork(
            store=self.store,
            workflows=workflows,
            steps=steps,
            clock=self.clock,
            converter=converter,
            config=config or TESTING_CONFIG,
            worker_options=worker_options or WorkerOptions(shutdown_grace=1.0),
        )
        self.auto_time_skipping = auto_time_skipping
        self._skip_task: asyncio.Task[None] | None = None
        self._mocked_originals: dict[str, StepDefinition[Any, Any]] = {}

    def __repr__(self) -> str:
        return f"WorkflowEnvironment(now={self.now().isoformat()!r})"

    # ------------------------------------------------------------------ lifecycle

    async def __aenter__(self) -> WorkflowEnvironment:
        await self.app.start()
        if self.auto_time_skipping:
            self._skip_task = asyncio.ensure_future(self._auto_skip_loop())
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._skip_task is not None:
            self._skip_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._skip_task
            self._skip_task = None
        await self.app.stop()

    # ------------------------------------------------------------------ time

    def now(self) -> datetime:
        """The environment's current (simulated) time."""
        return self.clock.now()

    async def sleep(self, duration: timedelta | float) -> datetime:
        """Advance the clock by ``duration`` (seconds, or a :class:`timedelta`).

        Useful with ``auto_time_skipping=False``, or to move time forward without
        waiting on any particular run (for example to let a schedule's catch-up window
        pass). Returns the new time.
        """
        return await self.clock.advance(duration)

    # ------------------------------------------------------------------ passthroughs

    @property
    def worker(self) -> Worker:
        return self.app.worker

    @property
    def registry(self) -> Registry:
        return self.app.registry

    @property
    def converter(self) -> DataConverter:
        return self.app.converter

    @property
    def client(self) -> Client:
        """The underlying :class:`~loomwork.Client`, for APIs this class does not wrap
        directly (schedules, listing executions, ``signal_with_start``, ...)."""
        return self.app.client

    def register(
        self, *definitions: WorkflowDefinition[..., Any] | StepDefinition[..., Any]
    ) -> None:
        """Register more definitions after construction (see :meth:`Loomwork.register`)."""
        self.app.register(*definitions)

    @overload
    async def start_workflow(
        self, workflow: WorkflowDefinition[..., R], *args: Any, id: str | None = None
    ) -> WorkflowHandle[R]: ...

    @overload
    async def start_workflow(
        self, workflow: str | type, *args: Any, id: str | None = None
    ) -> WorkflowHandle[Any]: ...

    async def start_workflow(
        self, workflow: WorkflowLike | str, *args: Any, id: str | None = None
    ) -> WorkflowHandle[Any]:
        return await self.app.start_workflow(workflow, *args, id=id)

    @overload
    async def execute(
        self,
        workflow: WorkflowDefinition[..., R],
        *args: Any,
        id: str | None = None,
        timeout: float | None = None,
    ) -> R: ...

    @overload
    async def execute(
        self, workflow: str | type, *args: Any, id: str | None = None, timeout: float | None = None
    ) -> Any: ...

    async def execute(
        self,
        workflow: WorkflowLike | str,
        *args: Any,
        id: str | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Start a run and wait for its result, with auto time-skipping doing its work.

        A ``timeout`` here is real wall-clock seconds (as a safety net against a test
        hanging, for example on a signal nothing ever sends) -- it has nothing to do
        with the environment's simulated time, which auto-skipping already fast-forwards
        through durable waits.
        """
        return await self.app.execute(workflow, *args, id=id, timeout=timeout)

    def get_handle(
        self,
        workflow_id: str | None = None,
        *,
        run_id: str | None = None,
        result_type: Any = None,
    ) -> WorkflowHandle[Any]:
        if result_type is None:
            return self.app.get_handle(workflow_id, run_id=run_id)
        return self.app.get_handle(workflow_id, run_id=run_id, result_type=result_type)

    # ------------------------------------------------------------------ interacting with a run

    async def signal(
        self,
        workflow_id: str,
        channel: Channel[Any] | str,
        value: Any,
        *,
        message_id: str | None = None,
    ) -> None:
        """Shortcut for ``(await get_handle(workflow_id)).signal(channel, value)``."""
        await self.get_handle(workflow_id).signal(channel, value, message_id=message_id)

    async def query(self, workflow_id: str, name: str, *args: Any, result_type: Any = None) -> Any:
        """Shortcut for ``(await get_handle(workflow_id)).query(name, *args)``."""
        return await self.get_handle(workflow_id).query(
            name, *args, result_type=result_type if result_type is not None else _NO_HINT
        )

    async def update(
        self,
        workflow_id: str,
        name: str,
        *args: Any,
        result_type: Any = None,
        timeout: float | None = None,
        update_id: str | None = None,
    ) -> Any:
        """Shortcut for ``(await get_handle(workflow_id)).update(name, *args)``."""
        return await self.get_handle(workflow_id).update(
            name,
            *args,
            result_type=result_type if result_type is not None else _NO_HINT,
            timeout=timeout,
            update_id=update_id,
        )

    async def cancel(self, workflow_id: str, reason: str = "") -> bool:
        """Shortcut for ``(await get_handle(workflow_id)).cancel(reason)``."""
        return await self.get_handle(workflow_id).cancel(reason)

    async def terminate(self, workflow_id: str, reason: str = "") -> bool:
        """Shortcut for ``(await get_handle(workflow_id)).terminate(reason)``."""
        return await self.get_handle(workflow_id).terminate(reason)

    async def list_executions(
        self,
        *,
        workflow: WorkflowDefinition[..., Any] | str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        """Shortcut for ``client.list(...)`` (newest first)."""
        return await self.app.client.list(workflow=workflow, status=status, limit=limit)

    # ------------------------------------------------------------------ mocking

    def mock_step(
        self,
        step: StepDefinition[Any, Any] | str,
        *,
        return_value: Any = UNSET,
        side_effect: Any = None,
    ) -> StepMock:
        """Replace ``step``'s implementation for this environment only.

        See :class:`~loomwork.testing.mocks.StepMock` for what ``return_value`` and
        ``side_effect`` accept, and for the assertions available on the returned mock.
        Steps that are never passed to ``mock_step`` keep running for real. Calling this
        again for the same step installs a fresh mock (with its own, empty call log) in
        place of the previous one; :meth:`unmock_step` restores the real implementation.
        """
        name = step.name if isinstance(step, StepDefinition) else step
        self._mocked_originals.setdefault(name, self.app.registry.step(name))
        return install_step_mock(
            self.app.registry, step, return_value=return_value, side_effect=side_effect
        )

    def unmock_step(self, step: StepDefinition[Any, Any] | str) -> None:
        """Restore the real implementation of a step previously replaced by :meth:`mock_step`.

        Raises:
            ValueError: this step is not currently mocked.
        """
        name = step.name if isinstance(step, StepDefinition) else step
        original = self._mocked_originals.pop(name, None)
        if original is None:
            raise ValueError(f"step {name!r} is not currently mocked")
        install_step(self.app.registry, original)

    # ------------------------------------------------------------------ history & golden files

    async def history(self, source: HistorySource) -> HistoryView:
        """Fetch (if needed) and wrap a run's history for inspection."""
        return HistoryView(await events_of(source))

    async def assert_history(self, source: HistorySource) -> HistoryAssertion:
        """Like :func:`loomwork.testing.assert_history`, using this environment's own
        registry and converter (so ``.no_nondeterminism()`` needs no arguments)."""
        events = await events_of(source)
        return HistoryAssertion(events, registry=self.app.registry, converter=self.app.converter)

    async def record_golden(self, source: HistorySource, path: str | Path) -> Path:
        """See :func:`loomwork.testing.record_golden`."""
        return await _record_golden(source, path)

    def assert_replays(self, path: str | Path, *workflows: WorkflowDefinition[..., Any]) -> None:
        """Like :func:`loomwork.testing.assert_replays`; defaults to every workflow this
        environment was built with when no ``workflows`` are given."""
        _assert_replays(path, *(workflows or self._all_workflows()), converter=self.app.converter)

    async def replay_check(
        self, source: HistorySource, *workflows: WorkflowDefinition[..., Any]
    ) -> None:
        """Like :func:`loomwork.testing.replay_check`; defaults to every workflow this
        environment was built with when no ``workflows`` are given."""
        await _replay_check(
            source, *(workflows or self._all_workflows()), converter=self.app.converter
        )

    def _all_workflows(self) -> tuple[WorkflowDefinition[..., Any], ...]:
        registry = self.app.registry
        return tuple(registry.workflow(name) for name in registry.workflow_names)

    # ------------------------------------------------------------------ auto time-skipping

    async def _snapshot(self) -> _Snapshot:
        store = self.store
        tick_at, step_at, timer_at, schedule_at, outbox = await asyncio.gather(
            store.next_tick_ready_at(),
            store.next_step_ready_at(),
            store.next_timer_at(),
            store.next_schedule_at(),
            store.outbox_size(),
        )
        return _Snapshot(self.worker.busy, tick_at, step_at, timer_at, schedule_at, outbox)

    async def _auto_skip_loop(self) -> None:
        while True:
            try:
                await self._auto_skip_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("auto time-skipping loop failed; retrying")
                await asyncio.sleep(_IDLE_FALLBACK_SECONDS)

    async def _auto_skip_once(self) -> None:
        snapshot = await self._snapshot()
        if snapshot.actionable(self.clock.now()):
            # Runnable work needs only a scheduler turn; a step doing real work (the
            # worker is busy) may take a while, so do not spin at full speed meanwhile.
            await asyncio.sleep(_BUSY_POLL_SECONDS if snapshot.busy else 0)
            return
        if not await self._settled():
            return
        snapshot = await self._snapshot()
        now = self.clock.now()
        if snapshot.actionable(now):
            return
        target = snapshot.next_deadline(now)
        if target is None:
            # Nothing pending anywhere: wait for something to be scheduled rather than
            # spin, but keep re-checking so a fresh run is picked up promptly.
            await self._idle_wait()
            return
        await self.clock.set(target)

    async def _settled(self) -> bool:
        """Give in-flight scheduling a few turns to finish, so a reading of "nothing is
        actionable" reflects the true state rather than a task that was just woken."""
        for _ in range(_STABILITY_YIELDS):
            await asyncio.sleep(0)
            if self.worker.busy:
                return False
        return True

    async def _idle_wait(self) -> None:
        notifier = self.store.notifier
        waits = [
            asyncio.ensure_future(
                notifier.wait(topic, notifier.token(topic), _IDLE_FALLBACK_SECONDS)
            )
            for topic in _IDLE_TOPICS
        ]
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in waits:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waits, return_exceptions=True)


__all__ = ["TESTING_CONFIG", "WorkflowEnvironment"]
