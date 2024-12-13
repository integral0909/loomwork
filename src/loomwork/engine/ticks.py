"""Executing workflow ticks.

A :class:`TickProcessor` claims a run with a pending tick, brings its (cached or fresh)
replayer up to date with the stored history, runs one live activation and commits the
resulting :class:`~loomwork.store.records.TickOutcome`.
"""

from __future__ import annotations

import logging
import random
from datetime import timedelta

from loomwork.clock import Clock
from loomwork.codec.converter import DataConverter
from loomwork.engine.cache import ReplayerCache
from loomwork.engine.config import EngineConfig
from loomwork.errors import LeaseLostError, NondeterminismError
from loomwork.history.events import TickCompleted, WorkflowStarted
from loomwork.ids import new_id
from loomwork.runtime.commands import (
    CancelTimer,
    CancelWorkflow,
    CompleteWorkflow,
    FailWorkflow,
    RequestCancelStep,
    ScheduleStep,
    StartTimer,
)
from loomwork.runtime.instance import WorkflowInstance
from loomwork.runtime.replayer import LiveTick, Replayer
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.records import (
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    StepJob,
    TickOutcome,
    TimerRecord,
)

logger = logging.getLogger("loomwork.engine.ticks")


class TickProcessor:
    """Claims and executes workflow ticks for the workflows in ``registry``."""

    def __init__(
        self,
        store: Store,
        registry: Registry,
        *,
        clock: Clock,
        converter: DataConverter,
        config: EngineConfig,
        worker_id: str,
    ) -> None:
        self.store = store
        self.registry = registry
        self.clock = clock
        self.converter = converter
        self.config = config
        self.worker_id = worker_id
        self.cache = ReplayerCache(config.replay_cache_size)
        self._rng = random.Random()
        self.ticks_completed = 0
        self.ticks_failed = 0

    async def run_once(self) -> bool:
        """Process at most one tick. Returns whether a tick was claimed."""
        names = self.registry.workflow_names
        if not names:
            return False
        record = await self.store.claim_tick(
            self.worker_id, self.clock.now(), self.config.tick_lease, names
        )
        if record is None:
            return False
        await self.process(record)
        return True

    def _make_instance(self, started: WorkflowStarted) -> WorkflowInstance:
        definition = self.registry.workflow(started.workflow_name)
        return WorkflowInstance(definition, started, converter=self.converter)

    async def process(self, record: ExecutionRecord) -> None:
        """Execute one tick of ``record``, which this worker must have leased."""
        run_id = record.run_id
        replayer = self.cache.take(run_id) or Replayer(self._make_instance)
        try:
            events = await self.store.load_history(run_id, after_id=replayer.last_event_id)
            replayer.feed(events)
            if replayer.is_closed:
                # The history is closed but the record still asked for a tick; nothing
                # can run. Release the lease without changing anything.
                replayer.close()
                await self._commit(
                    record, TickOutcome(self.worker_id, record.consumed_through, self.clock.now())
                )
                return
            live = replayer.live_tick(self.clock.now())
            outcome = self._build_outcome(record, live)
            committed = await self._commit(record, outcome)
        except LeaseLostError as exc:
            replayer.close()
            logger.info("tick for run %s abandoned: %s", run_id, exc)
            return
        except Exception as exc:
            replayer.close()
            await self._fail(record, exc)
            return

        self.ticks_completed += 1
        for event in committed:
            if isinstance(event, TickCompleted):
                replayer.acknowledge(event.event_id)
                break
        if outcome.close is not None:
            replayer.close()
        else:
            self.cache.put(run_id, replayer)

    async def _commit(self, record: ExecutionRecord, outcome: TickOutcome) -> list[object]:
        events = await self.store.complete_tick(
            record.run_id, self.worker_id, outcome, self.clock.now()
        )
        return list(events)

    def _build_outcome(self, record: ExecutionRecord, live: LiveTick) -> TickOutcome:
        now = self.clock.now()
        outcome = TickOutcome(
            worker_id=self.worker_id, consumed_through=live.consumed_through, time=live.time
        )
        new_steps: dict[int, StepJob] = {}
        new_timers: dict[int, TimerRecord] = {}
        for command in live.commands:
            outcome.decisions.append(command.to_event())
            match command:
                case ScheduleStep():
                    options = self.config.resolve_step_options(command.options)
                    deadline = now + options.total_timeout if options.total_timeout else None
                    new_steps[command.seq] = StepJob(
                        job_id=new_id(),
                        run_id=record.run_id,
                        workflow_id=record.workflow_id,
                        workflow_name=record.workflow_name,
                        seq=command.seq,
                        step_name=command.step_name,
                        args=list(command.args),
                        options=options,
                        scheduled_at=now,
                        ready_at=now,
                        deadline=deadline,
                    )
                case RequestCancelStep():
                    # Scheduled and cancelled within the same tick: never enqueue it.
                    if new_steps.pop(command.seq, None) is None:
                        outcome.cancel_steps.append(command.seq)
                case StartTimer():
                    new_timers[command.seq] = TimerRecord(
                        run_id=record.run_id, seq=command.seq, fire_at=command.fire_at
                    )
                case CancelTimer():
                    if new_timers.pop(command.seq, None) is None:
                        outcome.cancel_timers.append(command.seq)
                case CompleteWorkflow():
                    outcome.close = CloseOutcome(ExecutionStatus.COMPLETED, result=command.result)
                case FailWorkflow():
                    outcome.close = CloseOutcome(ExecutionStatus.FAILED, failure=command.failure)
                case CancelWorkflow():
                    outcome.close = CloseOutcome(ExecutionStatus.CANCELED, reason=command.reason)
                case _:
                    raise TypeError(f"unsupported command {command!r}")
        if outcome.close is None:
            outcome.new_steps = list(new_steps.values())
            outcome.new_timers = list(new_timers.values())
        return outcome

    async def _fail(self, record: ExecutionRecord, exc: Exception) -> None:
        self.ticks_failed += 1
        attempt = record.tick_failures + 1
        now = self.clock.now()
        delay: timedelta = self.config.tick_retry.delay_for(attempt, self._rng)
        message = f"{type(exc).__name__}: {exc}"
        level = logging.ERROR if isinstance(exc, NondeterminismError) else logging.WARNING
        logger.log(
            level,
            "tick %d for run %s (%s) failed, retrying in %.1fs: %s",
            attempt,
            record.run_id,
            record.workflow_name,
            delay.total_seconds(),
            message,
            exc_info=exc,
        )
        try:
            await self.store.fail_tick(record.run_id, self.worker_id, message, now + delay, now)
        except LeaseLostError:
            logger.info("lease on run %s lost before the tick failure was recorded", record.run_id)

    def close(self) -> None:
        self.cache.clear()
