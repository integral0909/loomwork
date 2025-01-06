"""Behavioural contract for :class:`loomwork.store.Store` implementations.

Backend test modules subclass :class:`StoreConformance` and provide a ``store`` fixture.
Every backend must pass every test here unchanged.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from loomwork.codec import default_converter
from loomwork.errors import LeaseLostError, NotFoundError, WorkflowAlreadyStartedError
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    CancelRequested,
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
from loomwork.store.base import Store
from loomwork.store.notify import TOPIC_STEPS, TOPIC_TICKS, TOPIC_TIMERS, closed_topic
from loomwork.store.records import (
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
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
