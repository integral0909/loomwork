"""Model-based test: the SQLite store behaves exactly like the in-memory store.

A Hypothesis state machine performs random sequences of store operations (starting runs,
delivering inputs, claiming and committing ticks with random decisions, executing and
retrying step jobs, firing timers, cancelling, terminating, relaying the outbox) against
a :class:`MemoryStore` and a :class:`SQLiteStore` side by side, and checks after every
step that both return the same observable results. The conformance suite specifies the
behaviour of individual operations; this test finds interleavings nobody thought to
write down.
"""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, precondition, rule

from loomwork.codec import default_converter
from loomwork.errors import (
    InvalidResetPointError,
    LeaseLostError,
    NotFoundError,
    WorkflowAlreadyStartedError,
    WorkflowIdReuseRejectedError,
)
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    SignalReceived,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TimerStarted,
    WorkflowCompleted,
    WorkflowStarted,
)
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint, StepOptions
from loomwork.store.base import Store
from loomwork.store.memory import MemoryStore
from loomwork.store.records import (
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    StepJob,
    TickOutcome,
    TimerRecord,
)
from loomwork.store.sqlite import SQLiteStore
from tests.helpers import T0

pytestmark = pytest.mark.slow

LEASE = timedelta(seconds=30)
PAYLOAD = default_converter.encode("v")
WORKFLOW_IDS = ["alpha", "beta", "gamma"]
WORKERS = ["w1", "w2"]


def normalize(value: Any) -> Any:
    """Reduce store results to what callers can observe and compare."""
    if isinstance(value, ExecutionRecord):
        return (
            value.run_id,
            value.workflow_id,
            value.status.value,
            value.last_event_id,
            value.last_input_id,
            value.consumed_through,
            value.tick_pending,
            value.tick_ready_at,
            value.tick_failures,
            value.lease_owner,
            value.cancel_requested,
            value.close_reason,
            value.continued_run_id,
            value.reset_from_run_id,
        )
    if isinstance(value, StepJob):
        return (
            value.job_id,
            value.run_id,
            value.seq,
            value.attempt,
            value.lease_owner,
            value.ready_at,
        )
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if isinstance(value, tuple):
        return tuple(normalize(v) for v in value)
    if hasattr(value, "kind") and hasattr(value, "event_id"):
        return (value.event_id, value.kind)
    return value


@dataclass
class Outcome:
    value: Any = None
    error: str | None = None


class StoreEquivalence(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.directory = tempfile.TemporaryDirectory()
        self.memory = MemoryStore()
        self.sqlite = SQLiteStore(Path(self.directory.name) / "model.db")
        self.run(self.sqlite.open())
        self.now = T0
        self.run_ids: list[str] = []
        self.counter = 0

    def run(self, coro: Any) -> Any:
        return self.loop.run_until_complete(coro)

    def teardown(self) -> None:
        self.run(self.sqlite.close())
        self.loop.close()
        self.directory.cleanup()

    def both(self, operation: Any) -> Outcome:
        """Apply ``operation(store)`` to both stores; they must agree."""
        outcomes = []
        for store in (self.memory, self.sqlite):
            try:
                outcomes.append(Outcome(normalize(self.run(operation(store)))))
            except (
                LeaseLostError,
                NotFoundError,
                WorkflowAlreadyStartedError,
                WorkflowIdReuseRejectedError,
                InvalidResetPointError,
                ValueError,
            ) as exc:
                outcomes.append(Outcome(error=type(exc).__name__))
        assert outcomes[0] == outcomes[1], f"stores disagree: {outcomes[0]} != {outcomes[1]}"
        return outcomes[0]

    def next_id(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}{self.counter:04d}"

    # ------------------------------------------------------------------ rules

    @initialize()
    def start(self) -> None:
        self.create_run(0)

    @rule(index=st.integers(0, len(WORKFLOW_IDS) - 1))
    def create_run(self, index: int) -> None:
        run_id = self.next_id("run")
        workflow_id = WORKFLOW_IDS[index]
        record = ExecutionRecord(
            run_id=run_id,
            workflow_id=workflow_id,
            workflow_name="wf",
            status=ExecutionStatus.RUNNING,
            created_at=self.now,
        )
        started = WorkflowStarted(
            workflow_name="wf", workflow_id=workflow_id, run_id=run_id, args=[PAYLOAD]
        )
        outcome = self.both(lambda s: s.create_execution(record, started, self.now))
        if outcome.error is None:
            self.run_ids.append(run_id)

    @rule(
        index=st.integers(0, len(WORKFLOW_IDS) - 1),
        id_reuse_policy=st.sampled_from(list(IdReusePolicy)),
        id_conflict_policy=st.sampled_from(list(IdConflictPolicy)),
    )
    def create_run_with_policy(
        self,
        index: int,
        id_reuse_policy: IdReusePolicy,
        id_conflict_policy: IdConflictPolicy,
    ) -> None:
        """Same as :meth:`create_run`, but exercising id reuse/conflict policies against
        whatever the workflow id's current state happens to be (running, closed, or
        never used) -- the interesting cases only arise from that existing state, which
        this rule does not otherwise control."""
        run_id = self.next_id("run")
        workflow_id = WORKFLOW_IDS[index]
        record = ExecutionRecord(
            run_id=run_id,
            workflow_id=workflow_id,
            workflow_name="wf",
            status=ExecutionStatus.RUNNING,
            created_at=self.now,
        )
        started = WorkflowStarted(
            workflow_name="wf", workflow_id=workflow_id, run_id=run_id, args=[PAYLOAD]
        )
        outcome = self.both(
            lambda s: s.create_execution(
                record,
                started,
                self.now,
                id_reuse_policy=id_reuse_policy,
                id_conflict_policy=id_conflict_policy,
            )
        )
        if outcome.error is None:
            self.run_ids.append(run_id)

    @precondition(lambda self: self.run_ids)
    @rule(
        data=st.data(),
        to_kind=st.sampled_from(["first_tick", "after_tick", "before_last_failure"]),
        tick_count=st.integers(0, 3),
    )
    def reset(self, data: st.DataObject, to_kind: str, tick_count: int) -> None:
        run_id = data.draw(st.sampled_from(self.run_ids))
        new_run_id = self.next_id("reset")
        to = (
            ResetPoint.first_tick()
            if to_kind == "first_tick"
            else ResetPoint.before_last_failure()
            if to_kind == "before_last_failure"
            else ResetPoint.after_tick(tick_count)
        )
        outcome = self.both(lambda s: s.reset_run(run_id, new_run_id, to, "model reset", self.now))
        if outcome.error is None:
            self.run_ids.append(new_run_id)

    @precondition(lambda self: self.run_ids)
    @rule(data=st.data(), kind=st.sampled_from(["signal", "cancel", "terminate"]))
    def external_input(self, data: st.DataObject, kind: str) -> None:
        run_id = data.draw(st.sampled_from(self.run_ids))
        if kind == "signal":
            event = SignalReceived(channel="c", payload=PAYLOAD, message_id=self.next_id("m"))
            self.both(lambda s: s.append_input(run_id, event, self.now))
        elif kind == "cancel":
            self.both(lambda s: s.request_cancel(run_id, "test", self.now))
        else:
            self.both(lambda s: s.terminate(run_id, "test", self.now))

    @rule(worker=st.sampled_from(WORKERS), data=st.data())
    def tick(self, worker: str, data: st.DataObject) -> None:
        claimed = self.both(lambda s: s.claim_tick(worker, self.now, LEASE))
        if claimed.value is None:
            return
        run_id = claimed.value[0]
        consumed = claimed.value[4]  # last_input_id: consume everything delivered
        action = data.draw(st.sampled_from(["step", "timer", "nothing", "complete", "fail"]))
        if action == "fail":
            self.both(
                lambda s: s.fail_tick(
                    run_id, worker, "boom", self.now + timedelta(seconds=5), self.now
                )
            )
            return
        seq = self.counter + 1
        self.counter += 1
        outcome = TickOutcome(worker_id=worker, consumed_through=consumed, time=self.now)
        if action == "step":
            outcome.decisions.append(StepScheduled(seq=seq, step_name="step"))
            outcome.new_steps.append(
                StepJob(
                    job_id=f"job{seq:04d}",
                    run_id=run_id,
                    workflow_id="?",
                    workflow_name="wf",
                    seq=seq,
                    step_name="step",
                    args=[PAYLOAD],
                    options=StepOptions(timeout=10),
                    scheduled_at=self.now,
                    ready_at=self.now,
                )
            )
        elif action == "timer":
            fire_at = self.now + timedelta(seconds=data.draw(st.integers(1, 120)))
            outcome.decisions.append(TimerStarted(seq=seq, fire_at=fire_at))
            outcome.new_timers.append(TimerRecord(run_id, seq, fire_at))
        elif action == "complete":
            outcome.decisions.append(WorkflowCompleted(result=PAYLOAD))
            outcome.close = CloseOutcome(ExecutionStatus.COMPLETED, result=PAYLOAD)
        self.both(lambda s: s.complete_tick(run_id, worker, outcome, self.now))

    @rule(worker=st.sampled_from(WORKERS), result=st.sampled_from(["ok", "fail", "retry"]))
    def step_attempt(self, worker: str, result: str) -> None:
        claimed = self.both(lambda s: s.claim_step(worker, self.now, LEASE))
        if claimed.value is None:
            return
        job_id, _, seq, attempt = claimed.value[:4]
        if result == "ok":
            event: StepCompleted | StepFailed = StepCompleted(
                seq=seq, result=PAYLOAD, attempts=attempt
            )
            self.both(lambda s: s.finish_step(job_id, worker, event, self.now))
        elif result == "fail":
            event = StepFailed(
                seq=seq, failure=FailureInfo(message="x", type="X"), attempts=attempt
            )
            self.both(lambda s: s.finish_step(job_id, worker, event, self.now))
        else:
            failure = FailureInfo(message="flaky", type="Flaky")
            ready = self.now + timedelta(seconds=3)
            self.both(lambda s: s.retry_step(job_id, worker, ready, failure, self.now))

    @rule(seconds=st.integers(1, 90))
    def advance(self, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)

    @rule()
    def fire_timers(self) -> None:
        self.both(lambda s: s.fire_due_timers(self.now))

    @rule()
    def relay(self) -> None:
        self.both(lambda s: s.relay_outbox(self.now))

    # ------------------------------------------------------------------ invariants

    @invariant()
    def same_executions(self) -> None:
        self.both(lambda s: s.list_executions(limit=1000))

    @invariant()
    def same_histories(self) -> None:
        for run_id in self.run_ids:
            self.both(lambda s, run_id=run_id: s.load_history(run_id))
            self.both(lambda s, run_id=run_id: s.get_step_jobs(run_id))

    @invariant()
    def same_timers(self) -> None:
        self.both(lambda s: s.next_timer_at())


def _store_type(store: Store) -> str:  # pragma: no cover - debugging aid
    return type(store).__name__


TestStoreEquivalence = StoreEquivalence.TestCase
TestStoreEquivalence.settings = settings(
    max_examples=40,
    stateful_step_count=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
