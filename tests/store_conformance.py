"""Behavioural contract for :class:`loomwork.store.Store` implementations.

Backend test modules subclass :class:`StoreConformance` and provide a ``store`` fixture.
Every backend must pass every test here unchanged.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from loomwork.codec import default_converter
from loomwork.errors import (
    LeaseLostError,
    NotFoundError,
    ScheduleConflictError,
    ScheduleExistsError,
    WorkflowAlreadyStartedError,
)
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    CancelRequested,
    ChildCompleted,
    ChildFailed,
    SignalReceived,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerFired,
    TimerStarted,
    WorkflowCompleted,
    WorkflowStarted,
    WorkflowTerminated,
)
from loomwork.options import StepOptions
from loomwork.schedules.model import (
    ScheduleAction,
    ScheduledRun,
    ScheduleSpec,
    ScheduleState,
)
from loomwork.store.base import Store
from loomwork.store.notify import TOPIC_STEPS, TOPIC_TICKS, TOPIC_TIMERS, closed_topic
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
from tests.helpers import T0

LEASE = timedelta(seconds=30)
PAYLOAD = default_converter.encode("x")


def record(run_id: str, workflow_id: str | None = None, name: str = "wf") -> ExecutionRecord:
    return ExecutionRecord(
        run_id=run_id,
        workflow_id=workflow_id or f"wid-{run_id}",
        workflow_name=name,
        status=ExecutionStatus.RUNNING,
        created_at=T0,
    )


def started(rec: ExecutionRecord) -> WorkflowStarted:
    return WorkflowStarted(
        workflow_name=rec.workflow_name,
        workflow_id=rec.workflow_id,
        run_id=rec.run_id,
        args=[PAYLOAD],
        random_seed=1,
    )


def job(run_id: str, seq: int, name: str = "step", *, job_id: str | None = None) -> StepJob:
    return StepJob(
        job_id=job_id or f"job-{run_id}-{seq}",
        run_id=run_id,
        workflow_id=f"wid-{run_id}",
        workflow_name="wf",
        seq=seq,
        step_name=name,
        args=[PAYLOAD],
        options=StepOptions(timeout=timedelta(seconds=5)),
        scheduled_at=T0,
        ready_at=T0,
    )


class StoreConformance:
    """Mix-in test class; subclasses provide the ``store`` fixture."""

    async def create(self, store: Store, run_id: str = "r1", **kwargs: str) -> ExecutionRecord:
        rec = record(run_id, **kwargs)
        created, _ = await store.create_execution(rec, started(rec), T0)
        return created

    async def claim(
        self, store: Store, worker: str = "w1", at: timedelta = timedelta(0)
    ) -> ExecutionRecord:
        claimed = await store.claim_tick(worker, T0 + at, LEASE)
        assert claimed is not None
        return claimed

    async def commit(
        self, store: Store, run_id: str, consumed: int, worker: str = "w1", **kwargs: object
    ) -> list[object]:
        outcome = TickOutcome(worker_id=worker, consumed_through=consumed, time=T0, **kwargs)  # type: ignore[arg-type]
        return list(await store.complete_tick(run_id, worker, outcome, T0))

    # ------------------------------------------------------------------ executions

    async def test_create_and_get(self, store: Store) -> None:
        created = await self.create(store)
        assert created.status is ExecutionStatus.RUNNING
        assert created.last_event_id == 1
        assert created.tick_pending
        fetched = await store.get_execution("r1")
        assert fetched.run_id == "r1"
        history = await store.load_history("r1")
        assert len(history) == 1
        assert isinstance(history[0], WorkflowStarted)
        assert history[0].event_id == 1
        assert history[0].timestamp == T0
        assert history[0].args == [PAYLOAD]

    async def test_get_missing(self, store: Store) -> None:
        with pytest.raises(NotFoundError):
            await store.get_execution("nope")
        with pytest.raises(NotFoundError):
            await store.find_execution("nope")
        with pytest.raises(NotFoundError):
            await store.load_history("nope")

    async def test_workflow_id_uniqueness_while_running(self, store: Store) -> None:
        await self.create(store, "r1", workflow_id="order-1")
        with pytest.raises(WorkflowAlreadyStartedError) as info:
            await self.create(store, "r2", workflow_id="order-1")
        assert info.value.run_id == "r1"
        await store.terminate("r1", "done", T0)
        await self.create(store, "r2", workflow_id="order-1")
        assert (await store.find_execution("order-1")).run_id == "r2"

    async def test_list_executions(self, store: Store) -> None:
        await self.create(store, "a", name="alpha")
        await self.create(store, "b", name="beta")
        await self.create(store, "c", name="alpha")
        await store.terminate("c", "", T0)
        assert [r.run_id for r in await store.list_executions()] == ["c", "b", "a"]
        assert [r.run_id for r in await store.list_executions(workflow_name="alpha")] == ["c", "a"]
        running = await store.list_executions(status=ExecutionStatus.RUNNING)
        assert [r.run_id for r in running] == ["b", "a"]
        assert len(await store.list_executions(limit=1)) == 1

    async def test_returned_records_are_copies(self, store: Store) -> None:
        created = await self.create(store)
        created.status = ExecutionStatus.FAILED
        assert (await store.get_execution("r1")).status is ExecutionStatus.RUNNING

    # ------------------------------------------------------------------ history

    async def test_load_history_after_and_limit(self, store: Store) -> None:
        await self.create(store)
        for seq in range(1, 5):
            await store.append_input("r1", TimerFired(seq=seq), T0)
        assert [e.event_id for e in await store.load_history("r1", after_id=2)] == [3, 4, 5]
        assert [e.event_id for e in await store.load_history("r1", after_id=1, limit=2)] == [2, 3]
        assert await store.load_history("r1", after_id=5) == []

    async def test_append_input_marks_tick_pending(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(store, "r1", consumed=1)
        assert not (await store.get_execution("r1")).tick_pending
        event = await store.append_input("r1", TimerFired(seq=1), T0 + timedelta(seconds=1))
        assert event is not None
        assert event.event_id == 3
        rec = await store.get_execution("r1")
        assert rec.tick_pending
        assert rec.last_input_id == 3
        assert rec.tick_ready_at == T0 + timedelta(seconds=1)

    async def test_append_input_rejects_decisions(self, store: Store) -> None:
        await self.create(store)
        with pytest.raises(ValueError, match="not an input"):
            await store.append_input("r1", WorkflowCompleted(result=PAYLOAD), T0)

    async def test_append_to_closed_run(self, store: Store) -> None:
        await self.create(store)
        await store.terminate("r1", "", T0)
        assert await store.append_input("r1", TimerFired(seq=1), T0) is None

    async def test_request_cancel_is_idempotent(self, store: Store) -> None:
        await self.create(store)
        assert await store.request_cancel("r1", "user", T0)
        assert await store.request_cancel("r1", "again", T0)
        history = await store.load_history("r1")
        cancels = [e for e in history if isinstance(e, CancelRequested)]
        assert len(cancels) == 1
        assert cancels[0].reason == "user"
        assert (await store.get_execution("r1")).cancel_requested

    async def test_terminate(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(
            store,
            "r1",
            consumed=1,
            new_steps=[job("r1", 1)],
            new_timers=[TimerRecord("r1", 1, T0 + timedelta(hours=1))],
        )
        assert await store.terminate("r1", "ops", T0)
        assert not await store.terminate("r1", "ops", T0)
        rec = await store.get_execution("r1")
        assert rec.status is ExecutionStatus.TERMINATED
        assert rec.close_reason == "ops"
        assert rec.closed_at == T0
        assert not rec.tick_pending
        assert isinstance((await store.load_history("r1"))[-1], WorkflowTerminated)
        assert await store.get_step_jobs("r1") == []
        assert await store.next_timer_at() is None
        assert not await store.request_cancel("r1", "", T0)

    # ------------------------------------------------------------------ ticks

    async def test_claim_tick_leases(self, store: Store) -> None:
        await self.create(store)
        claimed = await self.claim(store)
        assert claimed.lease_owner == "w1"
        assert claimed.lease_expires_at == T0 + LEASE
        assert await store.claim_tick("w2", T0, LEASE) is None
        # Expired leases can be taken over.
        stolen = await store.claim_tick("w2", T0 + LEASE, LEASE)
        assert stolen is not None
        assert stolen.lease_owner == "w2"

    async def test_claim_tick_filters_by_name(self, store: Store) -> None:
        await self.create(store, "r1", name="alpha")
        assert await store.claim_tick("w1", T0, LEASE, ["beta"]) is None
        assert await store.claim_tick("w1", T0, LEASE, ["alpha", "beta"]) is not None

    async def test_claim_tick_prefers_oldest_ready(self, store: Store) -> None:
        await self.create(store, "r1")
        await self.claim(store)
        await self.commit(store, "r1", consumed=1)
        await self.create(store, "r2")
        await store.append_input("r1", TimerFired(seq=1), T0 + timedelta(seconds=5))
        first = await store.claim_tick("w1", T0 + timedelta(seconds=10), LEASE)
        assert first is not None and first.run_id == "r2"

    async def test_complete_tick_appends_marker_and_decisions(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        decisions = [
            StepScheduled(seq=1, step_name="step", args=[PAYLOAD]),
            TimerStarted(seq=1, fire_at=T0 + timedelta(minutes=1)),
        ]
        committed = await self.commit(
            store,
            "r1",
            consumed=1,
            decisions=decisions,
            new_steps=[job("r1", 1)],
            new_timers=[TimerRecord("r1", 1, T0 + timedelta(minutes=1))],
        )
        assert [type(e) for e in committed] == [TickCompleted, StepScheduled, TimerStarted]
        tick_event = committed[0]
        assert isinstance(tick_event, TickCompleted)
        assert (tick_event.event_id, tick_event.consumed_through, tick_event.worker_id) == (
            2,
            1,
            "w1",
        )
        rec = await store.get_execution("r1")
        assert rec.lease_owner is None
        assert not rec.tick_pending
        assert rec.consumed_through == 1
        assert rec.last_event_id == 4
        assert [j.seq for j in await store.get_step_jobs("r1")] == [1]
        assert await store.next_timer_at() == T0 + timedelta(minutes=1)

    async def test_empty_tick_writes_nothing(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(store, "r1", consumed=1)
        # A cancel request arrives, but the next tick commits without consuming it
        # and without decisions (for example a worker that released the run early).
        await store.request_cancel("r1", "", T0)
        before = (await store.get_execution("r1")).last_event_id
        await self.claim(store)
        committed = await self.commit(store, "r1", consumed=1)
        assert committed == []
        rec = await store.get_execution("r1")
        assert rec.last_event_id == before
        assert rec.lease_owner is None
        assert rec.tick_pending  # the cancel request is still unconsumed

    async def test_inputs_during_tick_keep_it_pending(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await store.append_input("r1", TimerFired(seq=1), T0)  # arrives mid-tick
        await self.commit(store, "r1", consumed=1)
        rec = await store.get_execution("r1")
        assert rec.tick_pending
        history = await store.load_history("r1")
        assert [e.kind for e in history] == ["workflow_started", "timer_fired", "tick_completed"]

    async def test_complete_tick_requires_lease(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store, "w1")
        with pytest.raises(LeaseLostError):
            await self.commit(store, "r1", consumed=1, worker="w2")
        await store.claim_tick("w2", T0 + LEASE, LEASE)
        with pytest.raises(LeaseLostError):
            await self.commit(store, "r1", consumed=1, worker="w1")

    async def test_complete_tick_after_terminate(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await store.terminate("r1", "", T0)
        with pytest.raises(LeaseLostError):
            await self.commit(store, "r1", consumed=1)

    async def test_closing_tick(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(
            store, "r1", consumed=1, new_steps=[job("r1", 1)], new_timers=[TimerRecord("r1", 1, T0)]
        )
        await store.append_input("r1", TimerFired(seq=2), T0)
        await self.claim(store)
        await self.commit(
            store,
            "r1",
            consumed=4,
            decisions=[WorkflowCompleted(result=PAYLOAD)],
            new_steps=[job("r1", 2)],  # ignored because the run closes
            close=CloseOutcome(ExecutionStatus.COMPLETED, result=PAYLOAD),
        )
        rec = await store.get_execution("r1")
        assert rec.status is ExecutionStatus.COMPLETED
        assert rec.result == PAYLOAD
        assert rec.closed_at == T0
        assert not rec.tick_pending
        assert await store.get_step_jobs("r1") == []
        assert await store.next_timer_at() is None
        assert await store.claim_step("w1", T0, LEASE) is None

    async def test_fail_tick_schedules_retry(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        retry_at = T0 + timedelta(seconds=10)
        await store.fail_tick("r1", "w1", "boom", retry_at, T0)
        rec = await store.get_execution("r1")
        assert rec.tick_failures == 1
        assert rec.last_tick_error == "boom"
        assert rec.lease_owner is None
        assert rec.tick_ready_at == retry_at
        assert await store.claim_tick("w1", T0 + timedelta(seconds=9), LEASE) is None
        await self.claim(store, at=timedelta(seconds=10))
        await self.commit(store, "r1", consumed=1)
        rec = await store.get_execution("r1")
        assert rec.tick_failures == 0
        assert rec.last_tick_error is None

    async def test_fail_tick_requires_lease(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        with pytest.raises(LeaseLostError):
            await store.fail_tick("r1", "other", "x", T0, T0)

    async def test_cancel_steps_and_timers(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(
            store,
            "r1",
            consumed=1,
            new_steps=[job("r1", 1), job("r1", 2)],
            new_timers=[TimerRecord("r1", 1, T0), TimerRecord("r1", 2, T0 + timedelta(hours=1))],
        )
        running = await store.claim_step("w1", T0, LEASE)
        assert running is not None and running.seq == 1
        await store.append_input("r1", TimerFired(seq=99), T0)
        await self.claim(store)
        await self.commit(store, "r1", consumed=5, cancel_steps=[1, 2], cancel_timers=[1])
        # The unclaimed job is removed; the running one is marked and finishes normally.
        jobs = await store.get_step_jobs("r1")
        assert [(j.seq, j.cancel_requested) for j in jobs] == [(1, True)]
        assert await store.next_timer_at() == T0 + timedelta(hours=1)
        assert await store.claim_step("w2", T0 + LEASE, LEASE) is None
        assert await store.get_step_jobs("r1") == []  # abandoned + cancelled job cleaned up

    # ------------------------------------------------------------------ steps

    async def _with_jobs(self, store: Store, *jobs: StepJob) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(store, "r1", consumed=1, new_steps=list(jobs))

    async def test_claim_step_increments_attempt(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        assert claimed.attempt == 1
        assert claimed.lease_owner == "w1"
        assert await store.claim_step("w2", T0, LEASE) is None
        again = await store.claim_step("w2", T0 + LEASE, LEASE)
        assert again is not None
        assert again.attempt == 2

    async def test_claim_step_filters_names_and_readiness(self, store: Store) -> None:
        later = job("r1", 2, "late")
        later.ready_at = T0 + timedelta(seconds=5)
        await self._with_jobs(store, job("r1", 1, "a"), later)
        assert await store.claim_step("w1", T0, LEASE, ["late"]) is None
        claimed = await store.claim_step("w1", T0 + timedelta(seconds=5), LEASE, ["late"])
        assert claimed is not None and claimed.step_name == "late"

    async def test_finish_step_appends_result(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        ok = StepCompleted(seq=1, result=PAYLOAD, attempts=1)
        assert not await store.finish_step(claimed.job_id, "intruder", ok, T0)
        assert await store.finish_step(claimed.job_id, "w1", ok, T0)
        assert not await store.finish_step(claimed.job_id, "w1", ok, T0)
        history = await store.load_history("r1")
        assert isinstance(history[-1], StepCompleted)
        assert (await store.get_execution("r1")).tick_pending
        assert await store.get_step_jobs("r1") == []

    async def test_finish_step_on_closed_run_discards_result(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        await store.terminate("r1", "", T0)
        failed = StepFailed(seq=1, failure=FailureInfo(message="m", type="T"))
        assert not await store.finish_step(claimed.job_id, "w1", failed, T0)

    async def test_retry_step(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        failure = FailureInfo(message="flaky", type="Flaky")
        assert not await store.retry_step(claimed.job_id, "other", T0, failure, T0)
        assert await store.retry_step(claimed.job_id, "w1", T0 + timedelta(seconds=3), failure, T0)
        assert await store.claim_step("w1", T0 + timedelta(seconds=2), LEASE) is None
        again = await store.claim_step("w1", T0 + timedelta(seconds=3), LEASE)
        assert again is not None
        assert again.attempt == 2
        assert again.last_failure == failure

    async def test_renew_step_lease_can_shorten(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        assert await store.renew_step_lease(claimed.job_id, "w1", T0 + timedelta(seconds=1))
        assert await store.claim_step("w2", T0 + timedelta(seconds=1), LEASE) is not None

    async def test_renew_step_lease(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        assert await store.renew_step_lease(claimed.job_id, "w1", T0 + timedelta(hours=1))
        assert not await store.renew_step_lease(claimed.job_id, "w2", T0 + timedelta(hours=1))
        assert await store.claim_step("w2", T0 + LEASE, LEASE) is None
        assert await store.claim_step("w2", T0 + timedelta(hours=1), LEASE) is not None

    # ------------------------------------------------------------------ queues and heartbeats

    def queued(self, seq: int, queue: str, priority: int = 0, name: str = "step") -> StepJob:
        j = job("r1", seq, name)
        j.queue = queue
        j.priority = priority
        return j

    async def test_claim_step_filters_by_queue(self, store: Store) -> None:
        await self._with_jobs(store, self.queued(1, "default"), self.queued(2, "gpu"))
        claimed = await store.claim_step("w1", T0, LEASE, queues=["gpu"])
        assert claimed is not None
        assert (claimed.seq, claimed.queue) == (2, "gpu")
        assert await store.claim_step("w1", T0, LEASE, queues=["gpu"]) is None
        assert await store.claim_step("w1", T0, LEASE, queues=[]) is None
        other = await store.claim_step("w1", T0, LEASE, queues=None)
        assert other is not None and other.queue == "default"

    async def test_claim_step_prefers_higher_priority(self, store: Store) -> None:
        await self._with_jobs(
            store, self.queued(1, "q", 0), self.queued(2, "q", 10), self.queued(3, "q", -5)
        )
        order = []
        for _ in range(3):
            claimed = await store.claim_step("w1", T0, LEASE)
            assert claimed is not None
            order.append(claimed.seq)
        assert order == [2, 1, 3]

    async def test_heartbeat_renews_and_stores_details(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        details = default_converter.encode({"page": 7})
        result = await store.heartbeat_step(
            claimed.job_id, "w1", T0 + timedelta(minutes=5), details, T0 + timedelta(seconds=1)
        )
        assert result is not None and not result.cancel_requested
        assert await store.heartbeat_step(claimed.job_id, "w2", T0, None, T0) is None
        # The lease was renewed past the original expiry.
        assert await store.claim_step("w2", T0 + LEASE, LEASE) is None
        (stored,) = await store.get_step_jobs("r1")
        assert stored.heartbeat_details == details
        assert stored.last_heartbeat_at == T0 + timedelta(seconds=1)
        # A heartbeat without details keeps the previous details.
        await store.heartbeat_step(claimed.job_id, "w1", T0 + timedelta(minutes=5), None, T0)
        (stored,) = await store.get_step_jobs("r1")
        assert stored.heartbeat_details == details

    async def test_heartbeat_reports_cancellation(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        await store.append_input("r1", TimerFired(seq=77), T0)
        await self.claim(store)
        await self.commit(store, "r1", consumed=4, cancel_steps=[1])
        result = await store.heartbeat_step(claimed.job_id, "w1", T0 + LEASE, None, T0)
        assert result is not None and result.cancel_requested

    async def test_release_step_without_consuming_attempt(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None and claimed.attempt == 1
        assert not await store.release_step(claimed.job_id, "w2", T0, consume_attempt=False)
        assert await store.release_step(claimed.job_id, "w1", T0, consume_attempt=False)
        again = await store.claim_step("w2", T0, LEASE)
        assert again is not None
        assert again.attempt == 1

    async def test_release_step_consuming_attempt(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        assert await store.release_step(claimed.job_id, "w1", T0, consume_attempt=True)
        again = await store.claim_step("w2", T0, LEASE)
        assert again is not None and again.attempt == 2

    async def test_release_of_cancelled_job_deletes_it(self, store: Store) -> None:
        await self._with_jobs(store, job("r1", 1))
        claimed = await store.claim_step("w1", T0, LEASE)
        assert claimed is not None
        await store.append_input("r1", TimerFired(seq=77), T0)
        await self.claim(store)
        await self.commit(store, "r1", consumed=4, cancel_steps=[1])
        assert await store.release_step(claimed.job_id, "w1", T0, consume_attempt=True)
        assert await store.get_step_jobs("r1") == []

    async def test_next_step_ready_at(self, store: Store) -> None:
        later = self.queued(2, "gpu", name="render")
        later.ready_at = T0 + timedelta(seconds=30)
        await self._with_jobs(store, self.queued(1, "default"), later)
        assert await store.next_step_ready_at() == T0
        assert await store.next_step_ready_at(queues=["gpu"]) == T0 + timedelta(seconds=30)
        assert await store.next_step_ready_at(step_names=["render"]) == T0 + timedelta(seconds=30)
        assert await store.next_step_ready_at(queues=["none"]) is None
        assert await store.next_step_ready_at(queues=[]) is None
        # Leased jobs are not "ready".
        await store.claim_step("w1", T0, LEASE, queues=["default"])
        assert await store.next_step_ready_at(queues=["default"]) is None

    # ------------------------------------------------------------------ children and outbox

    def child_spec(
        self, parent: str, seq: int, workflow_id: str, policy: str = "terminate"
    ) -> ChildSpec:
        rec = record(f"{parent}.c{seq}", workflow_id=workflow_id, name="child")
        rec.parent_run_id = parent
        rec.parent_seq = seq
        rec.parent_close_policy = policy
        return ChildSpec(record=rec, started=started(rec))

    async def test_children_are_created_with_the_tick(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(store, "r1", consumed=1, new_children=[self.child_spec("r1", 1, "kid")])
        child = await store.get_execution("r1.c1")
        assert (child.parent_run_id, child.parent_seq, child.parent_close_policy) == (
            "r1",
            1,
            "terminate",
        )
        assert child.tick_pending
        assert [c.run_id for c in await store.list_children("r1")] == ["r1.c1"]
        assert await store.list_children("r1.c1") == []

    async def test_child_with_running_workflow_id_fails_to_start(self, store: Store) -> None:
        await self.create(store, "busy", workflow_id="kid")
        await self.create(store)
        await store.claim_tick("w1", T0, LEASE, ["wf"])  # claims one of them
        rec = await store.get_execution("r1")
        if rec.lease_owner is None:
            await store.complete_tick("busy", "w1", TickOutcome("w1", 1, T0), T0)
            await self.claim(store)
        await self.commit(store, "r1", consumed=1, new_children=[self.child_spec("r1", 1, "kid")])
        with pytest.raises(NotFoundError):
            await store.get_execution("r1.c1")
        history = await store.load_history("r1")
        failed = history[-1]
        assert isinstance(failed, ChildFailed)
        assert failed.failure.type == "ChildWorkflowAlreadyStarted"
        assert (await store.get_execution("r1")).tick_pending

    async def test_child_result_reaches_parent_through_outbox(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(store, "r1", consumed=1, new_children=[self.child_spec("r1", 1, "kid")])
        child_lease = await store.claim_tick("w2", T0, LEASE, ["child"])
        assert child_lease is not None
        await store.complete_tick(
            "r1.c1",
            "w2",
            TickOutcome(
                "w2",
                1,
                T0,
                decisions=[WorkflowCompleted(result=PAYLOAD)],
                close=CloseOutcome(ExecutionStatus.COMPLETED, result=PAYLOAD),
            ),
            T0,
        )
        assert await store.outbox_size() == 1
        assert await store.relay_outbox(T0) == 1
        assert await store.outbox_size() == 0
        last = (await store.load_history("r1"))[-1]
        assert isinstance(last, ChildCompleted)
        assert (last.seq, last.result) == (1, PAYLOAD)

    async def test_parent_close_policies(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(
            store,
            "r1",
            consumed=1,
            new_children=[
                self.child_spec("r1", 1, "k1", "terminate"),
                self.child_spec("r1", 2, "k2", "request_cancel"),
                self.child_spec("r1", 3, "k3", "abandon"),
            ],
        )
        await store.terminate("r1", "parent done", T0)
        assert await store.outbox_size() == 2
        await store.relay_outbox(T0)
        k1 = await store.get_execution("r1.c1")
        assert k1.status is ExecutionStatus.TERMINATED
        assert "parent run r1 closed" in k1.close_reason
        k2 = await store.get_execution("r1.c2")
        assert k2.status is ExecutionStatus.RUNNING and k2.cancel_requested
        assert (await store.get_execution("r1.c3")).status is ExecutionStatus.RUNNING
        # The terminated child reports back to its (closed) parent; the relay drops it.
        assert await store.outbox_size() == 1
        assert await store.relay_outbox(T0) == 1
        assert not any(isinstance(e, ChildFailed) for e in await store.load_history("r1"))

    async def test_outbox_signal_by_workflow_id(self, store: Store) -> None:
        await self.create(store, "target", workflow_id="orders-1")
        await self.create(store)
        await store.claim_tick("w1", T0, LEASE, ["wf"])
        rec = await store.get_execution("r1")
        worker_run = "r1" if rec.lease_owner else "target"
        signal = SignalReceived(channel="c", payload=PAYLOAD, message_id="m1")
        await store.complete_tick(
            worker_run,
            "w1",
            TickOutcome(
                "w1",
                1,
                T0,
                outbox=[
                    OutboxMessage(
                        OutboxAction.APPEND_INPUT, target_workflow_id="orders-1", event=signal
                    ),
                    OutboxMessage(
                        OutboxAction.APPEND_INPUT, target_workflow_id="missing", event=signal
                    ),
                ],
            ),
            T0,
        )
        assert await store.relay_outbox(T0, limit=1) == 1
        assert await store.relay_outbox(T0) == 1
        delivered = (await store.load_history("target"))[-1]
        assert isinstance(delivered, SignalReceived)
        assert delivered.message_id == "m1"
        assert delivered.event_id > 1
        assert (await store.get_execution("target")).tick_pending

    async def test_create_execution_with_extra_inputs(self, store: Store) -> None:
        rec = record("r1")
        signal = SignalReceived(channel="c", payload=PAYLOAD)
        await store.create_execution(rec, started(rec), T0, extra_inputs=[signal])
        history = await store.load_history("r1")
        assert [e.kind for e in history] == ["workflow_started", "signal_received"]
        assert (await store.get_execution("r1")).last_input_id == 2
        other = record("r2")
        bad_extra = [WorkflowCompleted(result=PAYLOAD)]
        with pytest.raises(ValueError, match="not an input"):
            await store.create_execution(other, started(other), T0, extra_inputs=bad_extra)

    def test_outbox_message_validation(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            OutboxMessage(OutboxAction.TERMINATE)
        with pytest.raises(ValueError, match="input event"):
            OutboxMessage(OutboxAction.APPEND_INPUT, target_run_id="x")

    # ------------------------------------------------------------------ schedules

    def schedule(self, schedule_id: str = "s1", **changes: object) -> ScheduleState:
        base = ScheduleState(
            schedule_id=schedule_id,
            spec=ScheduleSpec(cron="0 * * * *"),
            action=ScheduleAction("wf", [PAYLOAD]),
            next_fire_at=T0 + timedelta(hours=1),
        )
        return base.with_(**changes)

    async def test_schedule_crud(self, store: Store) -> None:
        created = await store.create_schedule(self.schedule(note="hello"), T0)
        assert created.version == 1
        assert created.created_at == T0
        with pytest.raises(ScheduleExistsError):
            await store.create_schedule(self.schedule(), T0)
        fetched = await store.get_schedule("s1")
        assert fetched == created
        assert fetched.action.args == [PAYLOAD]
        updated = await store.update_schedule(fetched.with_(note="changed"), 1, T0)
        assert updated.version == 2
        with pytest.raises(ScheduleConflictError):
            await store.update_schedule(fetched.with_(note="stale"), 1, T0)
        assert (await store.get_schedule("s1")).note == "changed"
        await store.create_schedule(self.schedule("a0"), T0)
        assert [s.schedule_id for s in await store.list_schedules()] == ["a0", "s1"]
        assert await store.delete_schedule("s1")
        assert not await store.delete_schedule("s1")
        with pytest.raises(NotFoundError):
            await store.get_schedule("s1")
        with pytest.raises(NotFoundError):
            await store.update_schedule(self.schedule(), 1, T0)

    async def test_due_schedules_and_next_time(self, store: Store) -> None:
        await store.create_schedule(
            self.schedule("early", next_fire_at=T0 + timedelta(minutes=5)), T0
        )
        await store.create_schedule(self.schedule("late", next_fire_at=T0 + timedelta(hours=2)), T0)
        await store.create_schedule(
            self.schedule("paused", paused=True, next_fire_at=T0 + timedelta(minutes=1)), T0
        )
        await store.create_schedule(
            self.schedule("buffered", buffered=T0, next_fire_at=T0 + timedelta(days=1)), T0
        )
        assert await store.next_schedule_at() == T0 + timedelta(minutes=5)
        due = await store.due_schedules(T0 + timedelta(minutes=10))
        assert [s.schedule_id for s in due] == ["buffered", "early"]

    async def test_fire_schedule_starts_runs_atomically(self, store: Store) -> None:
        created = await store.create_schedule(self.schedule(), T0)
        rec = record("sched-run-1", workflow_id="s1@1")
        run = ScheduledRun(T0, T0, "s1@1", "sched-run-1")
        fired = await store.fire_schedule(
            created.with_(running=(run,), recent=(run,), total_started=1),
            1,
            [ChildSpec(rec, started(rec))],
            [],
            T0,
        )
        assert fired.version == 2
        assert fired.running == (run,)
        assert (await store.get_execution("sched-run-1")).tick_pending
        with pytest.raises(ScheduleConflictError):
            await store.fire_schedule(fired, 1, [], [], T0)

    async def test_fire_schedule_skips_running_workflow_ids(self, store: Store) -> None:
        await self.create(store, "existing", workflow_id="s1@1")
        created = await store.create_schedule(self.schedule(), T0)
        rec = record("dup-run", workflow_id="s1@1")
        run = ScheduledRun(T0, T0, "s1@1", "dup-run")
        fired = await store.fire_schedule(
            created.with_(running=(run,), recent=(run,), total_started=1),
            1,
            [ChildSpec(rec, started(rec))],
            [OutboxMessage(OutboxAction.TERMINATE, target_run_id="existing", reason="newer")],
            T0,
        )
        assert fired.running == ()
        assert fired.recent[-1].status == "already_running"
        assert fired.total_started == 0
        with pytest.raises(NotFoundError):
            await store.get_execution("dup-run")
        assert await store.outbox_size() == 1

    # ------------------------------------------------------------------ timers

    async def test_fire_due_timers(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(
            store,
            "r1",
            consumed=1,
            new_timers=[
                TimerRecord("r1", 1, T0 + timedelta(seconds=10)),
                TimerRecord("r1", 2, T0 + timedelta(seconds=5)),
                TimerRecord("r1", 3, T0 + timedelta(seconds=20)),
            ],
        )
        assert await store.next_timer_at() == T0 + timedelta(seconds=5)
        assert await store.fire_due_timers(T0 + timedelta(seconds=4)) == 0
        assert await store.fire_due_timers(T0 + timedelta(seconds=10), limit=1) == 1
        assert await store.fire_due_timers(T0 + timedelta(seconds=10)) == 1
        fired = [e for e in await store.load_history("r1") if isinstance(e, TimerFired)]
        assert [e.seq for e in fired] == [2, 1]
        assert await store.next_timer_at() == T0 + timedelta(seconds=20)
        assert (await store.get_execution("r1")).tick_pending

    async def test_cancelled_timer_never_fires(self, store: Store) -> None:
        await self.create(store)
        await self.claim(store)
        await self.commit(store, "r1", consumed=1, new_timers=[TimerRecord("r1", 1, T0)])
        await store.append_input("r1", TimerFired(seq=50), T0)
        await self.claim(store)
        await self.commit(store, "r1", consumed=4, cancel_timers=[1])
        assert await store.fire_due_timers(T0 + timedelta(days=1)) == 0
        assert await store.next_timer_at() is None

    # ------------------------------------------------------------------ retention

    async def test_delete_closed_before(self, store: Store) -> None:
        await self.create(store, "old", workflow_id="w")
        await self.create(store, "running")
        await store.terminate("old", "", T0)
        await self.create(store, "new", workflow_id="w")
        await store.terminate("new", "", T0 + timedelta(days=2))
        assert await store.delete_closed_before(T0 + timedelta(days=1)) == 1
        with pytest.raises(NotFoundError):
            await store.get_execution("old")
        with pytest.raises(NotFoundError):
            await store.load_history("old")
        assert (await store.find_execution("w")).run_id == "new"
        assert {r.run_id for r in await store.list_executions()} == {"new", "running"}
        assert await store.delete_closed_before(T0 + timedelta(days=30), limit=5) == 1
        assert [r.run_id for r in await store.list_executions()] == ["running"]

    # ------------------------------------------------------------------ notifications

    async def test_notifications(self, store: Store) -> None:
        notifier = store.notifier
        ticks = notifier.token(TOPIC_TICKS)
        await self.create(store)
        assert await notifier.wait(TOPIC_TICKS, ticks, 0.01)

        steps, timers = notifier.token(TOPIC_STEPS), notifier.token(TOPIC_TIMERS)
        await self.claim(store)
        await self.commit(
            store, "r1", consumed=1, new_steps=[job("r1", 1)], new_timers=[TimerRecord("r1", 1, T0)]
        )
        assert await notifier.wait(TOPIC_STEPS, steps, 0.01)
        assert await notifier.wait(TOPIC_TIMERS, timers, 0.01)

        closed = notifier.token(closed_topic("r1"))
        waiter = asyncio.ensure_future(notifier.wait(closed_topic("r1"), closed, 5))
        await asyncio.sleep(0)
        await store.terminate("r1", "", T0)
        assert await asyncio.wait_for(waiter, 1)
