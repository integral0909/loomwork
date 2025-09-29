"""Executing workflow ticks.

A :class:`TickProcessor` claims a run with a pending tick, brings its (cached or fresh)
replayer up to date with the stored history, runs one live activation and commits the
resulting :class:`~loomwork.store.records.TickOutcome`.
"""

from __future__ import annotations

import dataclasses
import logging
import random
import secrets
import time
from datetime import datetime, timedelta
from typing import Any

from loomwork.clock import Clock
from loomwork.codec.converter import DataConverter
from loomwork.codec.transform import CodecBoundary
from loomwork.engine.cache import ReplayerCache
from loomwork.engine.config import EngineConfig
from loomwork.errors import HistoryLimitExceeded, LeaseLostError, NondeterminismError
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    Event,
    SignalReceived,
    TickCompleted,
    WorkflowFailed,
    WorkflowStarted,
)
from loomwork.ids import new_id
from loomwork.observability.telemetry import NoopTelemetry, Telemetry, TickReport
from loomwork.retry import RetryPolicy
from loomwork.runtime.commands import (
    CancelTimer,
    CancelWorkflow,
    CompleteUpdate,
    CompleteWorkflow,
    ContinueAsNewCommand,
    FailWorkflow,
    RecordPatch,
    RequestCancelStep,
    ScheduleChild,
    ScheduleStep,
    SignalExternal,
    StartTimer,
    UpsertSearchAttributes,
)
from loomwork.runtime.instance import WorkflowInstance
from loomwork.runtime.replayer import LiveTick, Replayer
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.records import (
    ChildSpec,
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    OutboxAction,
    OutboxMessage,
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
        telemetry: Telemetry | None = None,
    ) -> None:
        self.telemetry: Telemetry = telemetry or NoopTelemetry()
        self.store = store
        self.registry = registry
        self.clock = clock
        self.converter = converter
        self.config = config
        self.worker_id = worker_id
        self.cache = ReplayerCache(config.replay_cache_size)
        self.boundary = CodecBoundary(converter.codec)
        self._rng = random.Random()
        self.ticks_completed = 0
        self.ticks_failed = 0
        # Best-effort, this worker's own memory: which runs it has already reported a
        # history-size warning for, per unit. History growth also happens outside of
        # tick processing (a step or timer outcome arriving is a plain append), so the
        # threshold can already be crossed by the time a tick is next claimed; checking
        # a persisted "warned" flag would need a store round trip or a schema field for
        # a purely informational signal, so this checks the record already in hand
        # instead and accepts that a run reprocessed by a different worker (or one whose
        # cache of this set was never populated) may warn again.
        self._warned_events: set[str] = set()
        self._warned_bytes: set[str] = set()

    async def claim(self) -> ExecutionRecord | None:
        """Lease a run with a pending tick of a registered workflow, if there is one."""
        names = self.registry.workflow_names
        if not names:
            return None
        return await self.store.claim_tick(
            self.worker_id, self.clock.now(), self.config.tick_lease, names
        )

    async def run_once(self) -> bool:
        """Process at most one tick. Returns whether a tick was claimed."""
        record = await self.claim()
        if record is None:
            return False
        await self.process(record)
        return True

    def _make_instance(self, started: WorkflowStarted) -> WorkflowInstance:
        definition = self.registry.workflow(started.workflow_name)
        instance = WorkflowInstance(definition, started, converter=self.converter)
        instance.continue_as_new_suggested_at = self.config.continue_as_new_after_events
        return instance

    async def process(self, record: ExecutionRecord) -> None:
        """Execute one tick of ``record``, which this worker must have leased."""
        started = time.perf_counter()
        cached = self.cache.take(record.run_id)
        replayer = cached or Replayer(self._make_instance)
        replayed_before = replayer.replayed_ticks
        outcome_name, commands, error = await self._process(record, replayer)
        self.telemetry.tick_finished(
            TickReport(
                run_id=record.run_id,
                workflow_name=record.workflow_name,
                duration=time.perf_counter() - started,
                outcome=outcome_name,
                commands=commands,
                replayed_ticks=replayer.replayed_ticks - replayed_before,
                cache_hit=cached is not None,
                error=error,
            )
        )

    async def _process(
        self, record: ExecutionRecord, replayer: Replayer
    ) -> tuple[str, int, str | None]:
        """Run the tick; returns (outcome, number of commands, error message)."""
        run_id = record.run_id
        commands = 0
        self._check_history_warning(record)
        try:
            events = await self.store.load_history(run_id, after_id=replayer.last_event_id)
            replayer.feed(await self.boundary.decode(events))
            if replayer.is_closed:
                # The history is closed but the record still asked for a tick; nothing
                # can run. Release the lease without changing anything.
                replayer.close()
                await self._commit(
                    record, TickOutcome(self.worker_id, record.consumed_through, self.clock.now())
                )
                return "closed", 0, None
            outcome = self._history_limit_outcome(record)
            if outcome is None:
                live = replayer.live_tick(self.clock.now())
                if self.boundary.active:
                    live = dataclasses.replace(
                        live, commands=await self.boundary.encode(live.commands)
                    )
                outcome = self._build_outcome(record, live, replayer.instance)
                commands = len(live.commands)
            committed = await self._commit(record, outcome)
        except LeaseLostError as exc:
            replayer.close()
            logger.info("tick for run %s abandoned: %s", run_id, exc)
            return "lease_lost", 0, str(exc)
        except Exception as exc:
            replayer.close()
            await self._fail(record, exc)
            return "failed", 0, f"{type(exc).__name__}: {exc}"

        self.ticks_completed += 1
        for event in committed:
            if isinstance(event, TickCompleted):
                replayer.acknowledge(event.event_id)
                break
        if outcome.close is not None:
            replayer.close()
            self._warned_events.discard(run_id)
            self._warned_bytes.discard(run_id)
        else:
            self.cache.put(run_id, replayer)
        if not committed:
            return "empty", 0, None
        return (
            ("completed" if outcome.close is not None else "progressed"),
            commands,
            None,
        )

    def _history_limit_outcome(self, record: ExecutionRecord) -> TickOutcome | None:
        """A synthetic failing tick if ``record`` already reached its history limit.

        Checked against the history size as of the *last committed* tick (before this
        one runs), so the decision is deterministic and identical across retries: no
        workflow code executes, so nothing about it can vary with the worker or the
        wall clock. A run that reaches the limit stays failed; every later claim of its
        (now non-running) execution is impossible, so this never re-fires.
        """
        if record.last_event_id >= self.config.history_limit_events:
            size, unit = record.last_event_id, "events"
        elif (
            self.config.history_limit_bytes is not None
            and record.history_bytes >= self.config.history_limit_bytes
        ):
            size, unit = record.history_bytes, "bytes"
        else:
            return None
        failure = FailureInfo.from_exception(
            HistoryLimitExceeded(
                f"history reached the configured limit of {size} {unit}; call "
                "loomwork.continue_as_new() well before this point to keep it bounded"
            )
        )
        logger.error(
            "run %s of %s failed: history limit reached (%d %s)",
            record.run_id,
            record.workflow_name,
            size,
            unit,
        )
        return TickOutcome(
            worker_id=self.worker_id,
            consumed_through=record.consumed_through,
            time=self.clock.now(),
            decisions=[WorkflowFailed(failure=failure)],
            close=CloseOutcome(ExecutionStatus.FAILED, failure=failure),
        )

    def _check_history_warning(self, record: ExecutionRecord) -> None:
        """Report ``history_warning_events``/``history_warning_bytes``, at most once per
        run per unit, using only the (already committed) counts on ``record`` -- the
        state as of this tick's claim, which reflects every append so far, whether from
        a tick or a step/timer outcome arriving on its own."""
        run_id = record.run_id
        if record.last_event_id >= self.config.history_warning_events and run_id not in (
            self._warned_events
        ):
            self._warned_events.add(run_id)
            self.telemetry.history_size_warning(
                run_id, record.workflow_name, record.last_event_id, "events"
            )
        warn_bytes = self.config.history_warning_bytes
        if (
            warn_bytes is not None
            and record.history_bytes >= warn_bytes
            and run_id not in self._warned_bytes
        ):
            self._warned_bytes.add(run_id)
            self.telemetry.history_size_warning(
                run_id, record.workflow_name, record.history_bytes, "bytes"
            )

    async def _commit(self, record: ExecutionRecord, outcome: TickOutcome) -> list[Event]:
        events = await self.store.complete_tick(
            record.run_id, self.worker_id, outcome, self.clock.now()
        )
        return list(events)

    def _build_outcome(
        self, record: ExecutionRecord, live: LiveTick, instance: WorkflowInstance | None
    ) -> TickOutcome:
        now = self.clock.now()
        outcome = TickOutcome(
            worker_id=self.worker_id,
            consumed_through=live.consumed_through,
            time=live.time,
            code_version=self._code_version(record.workflow_name),
        )
        new_steps: dict[int, StepJob] = {}
        new_timers: dict[int, TimerRecord] = {}
        for issued in live.commands:
            command = issued
            if isinstance(command, ContinueAsNewCommand):
                # The successor's run id is assigned now, at commit, and recorded.
                command = dataclasses.replace(command, new_run_id=new_id())
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
                        queue=options.effective_queue,
                        priority=options.effective_priority,
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
                case ScheduleChild():
                    outcome.new_children.append(self._child_spec(record, command, now))
                case SignalExternal():
                    outcome.outbox.append(
                        OutboxMessage(
                            action=OutboxAction.APPEND_INPUT,
                            target_workflow_id=command.workflow_id,
                            event=SignalReceived(
                                channel=command.channel,
                                payload=command.payload,
                                message_id=command.message_id,
                                sender=record.workflow_id,
                            ),
                        )
                    )
                case CompleteUpdate() | RecordPatch():
                    pass  # recorded as a decision event only; no side effect
                case UpsertSearchAttributes():
                    outcome.search_attributes = {
                        **(outcome.search_attributes or {}),
                        **command.attributes,
                    }
                case ContinueAsNewCommand():
                    outcome.close = CloseOutcome(
                        ExecutionStatus.CONTINUED_AS_NEW, continued_run_id=command.new_run_id
                    )
                    outcome.continuation = self._continuation(
                        record, command, now, instance.started if instance else None
                    )
                case CompleteWorkflow():
                    outcome.close = CloseOutcome(ExecutionStatus.COMPLETED, result=command.result)
                case FailWorkflow():
                    outcome.close = CloseOutcome(ExecutionStatus.FAILED, failure=command.failure)
                    if instance is not None:
                        self._maybe_retry(record, command, instance.started, outcome, now)
                case CancelWorkflow():
                    outcome.close = CloseOutcome(ExecutionStatus.CANCELED, reason=command.reason)
                case _:
                    raise TypeError(f"unsupported command {command!r}")
        if outcome.close is None:
            outcome.new_steps = list(new_steps.values())
            outcome.new_timers = list(new_timers.values())
        return outcome

    def _code_version(self, workflow_name: str) -> str:
        if not self.registry.has_workflow(workflow_name):
            return ""
        return self.registry.workflow(workflow_name).fingerprint

    @staticmethod
    def _child_spec(parent: ExecutionRecord, command: ScheduleChild, now: Any) -> ChildSpec:
        child = ExecutionRecord(
            run_id=command.run_id,
            workflow_id=command.workflow_id,
            workflow_name=command.workflow_name,
            status=ExecutionStatus.RUNNING,
            created_at=now,
            parent_run_id=parent.run_id,
            parent_seq=command.seq,
            parent_close_policy=command.parent_close_policy,
        )
        started = WorkflowStarted(
            workflow_name=command.workflow_name,
            workflow_id=command.workflow_id,
            run_id=command.run_id,
            args=list(command.args),
            random_seed=secrets.randbits(63),
            parent_workflow_id=parent.workflow_id,
            parent_run_id=parent.run_id,
        )
        return ChildSpec(record=child, started=started)

    def _maybe_retry(
        self,
        record: ExecutionRecord,
        command: FailWorkflow,
        started: WorkflowStarted,
        outcome: TickOutcome,
        now: datetime,
    ) -> None:
        """Turn a failed run into a retry if its workflow retry policy allows one."""
        if started.retry_policy is None:
            return
        policy = RetryPolicy.from_dict(started.retry_policy)
        if not policy.should_retry(started.attempt, command.failure):
            return
        ready_at = now + policy.delay_for(started.attempt, self._rng)
        if started.execution_deadline is not None and ready_at >= started.execution_deadline:
            return
        new_run_id = new_id()
        successor = self._continuation(
            record,
            ContinueAsNewCommand(
                workflow_name=record.workflow_name, args=list(started.args), new_run_id=new_run_id
            ),
            now,
            started,
        )
        assert isinstance(successor.started, WorkflowStarted)
        retried_start = dataclasses.replace(
            successor.started,
            attempt=started.attempt + 1,
            search_attributes=dict(started.search_attributes),
        )
        outcome.close = CloseOutcome(
            ExecutionStatus.FAILED, failure=command.failure, continued_run_id=new_run_id
        )
        outcome.continuation = ChildSpec(record=successor.record, started=retried_start)
        outcome.continuation_ready_at = ready_at
        logger.info(
            "run %s of %s failed (attempt %d); retrying as %s at %s",
            record.run_id,
            record.workflow_name,
            started.attempt,
            new_run_id,
            ready_at.isoformat(),
        )

    @staticmethod
    def _continuation(
        record: ExecutionRecord,
        command: ContinueAsNewCommand,
        now: Any,
        previous: WorkflowStarted | None = None,
    ) -> ChildSpec:
        """The successor run: same workflow id and parent, fresh history.

        Timeouts and the retry policy carry over, so they bound the whole chain.
        """
        successor = ExecutionRecord(
            run_id=command.new_run_id,
            workflow_id=record.workflow_id,
            workflow_name=command.workflow_name,
            status=ExecutionStatus.RUNNING,
            created_at=now,
            parent_run_id=record.parent_run_id,
            parent_seq=record.parent_seq,
            parent_close_policy=record.parent_close_policy,
        )
        started = WorkflowStarted(
            workflow_name=command.workflow_name,
            workflow_id=record.workflow_id,
            run_id=command.new_run_id,
            args=list(command.args),
            random_seed=secrets.randbits(63),
            continued_from=record.run_id,
            run_timeout=previous.run_timeout if previous else None,
            execution_deadline=previous.execution_deadline if previous else None,
            retry_policy=previous.retry_policy if previous else None,
        )
        return ChildSpec(record=successor, started=started)

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
