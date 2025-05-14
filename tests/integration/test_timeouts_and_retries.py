"""Run timeouts, execution timeouts and workflow retry policies."""

from __future__ import annotations

from datetime import timedelta

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock, MemoryStore
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration

RETRY = lw.RetryPolicy(initial_interval=10, backoff=2, max_attempts=3, jitter=0)


@lw.workflow(name="to.sleeper")
async def sleeper(minutes: int) -> str:
    await lw.sleep(timedelta(minutes=minutes))
    return "woke"


@lw.workflow(name="to.flaky")
async def flaky(succeed_on: int) -> str:
    attempt = lw.info().attempt
    if attempt < succeed_on:
        raise lw.ApplicationError(f"attempt {attempt} failed", type="Transient")
    return f"succeeded on attempt {attempt}"


@lw.workflow(name="to.fatal")
async def fatal() -> None:
    raise lw.ApplicationError("never retry", type="Fatal", non_retryable=True)


@lw.workflow(name="to.looping")
async def looping(rounds: int) -> str:
    await lw.sleep(timedelta(minutes=10))
    if rounds:
        lw.continue_as_new(rounds - 1)
    return "done"


@lw.workflow(name="to.parent")
async def parent() -> str:
    try:
        await lw.start_child(flaky, 2)
    except lw.ChildWorkflowError as exc:
        return f"child {exc.status}"
    return "child ok"


def make(clock: ManualClock, store: MemoryStore) -> lw.Loomwork:
    return lw.Loomwork(
        store=store,
        workflows=[sleeper, flaky, fatal, looping, parent],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )


async def test_run_timeout(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.start_workflow(sleeper, 60, run_timeout=timedelta(minutes=30))
        await eventually(lambda: store.timer_count == 2)
        await clock.advance(timedelta(minutes=30))
        with pytest.raises(lw.WorkflowTimedOutError, match="timed out"):
            await handle.result(timeout=5)
        assert (await handle.describe()).status is ExecutionStatus.TIMED_OUT
        assert store.timer_count == 0


async def test_execution_timeout_spans_continue_as_new(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.start_workflow(
            looping, 5, id="looper", execution_timeout=timedelta(minutes=25)
        )
        for run in range(1, 4):
            # Each run has its sleep timer and the chain's deadline timer.
            await eventually(lambda run=run: store.run_count == run and store.timer_count == 2)
            await clock.advance(timedelta(minutes=10))
        with pytest.raises(lw.WorkflowTimedOutError):
            await handle.result(timeout=5)
        runs = sorted(
            await store.list_executions(workflow_name="to.looping"), key=lambda r: r.created_at
        )
        # Runs 1 and 2 continued as new at 10 and 20 minutes; run 3 hit the 25-minute
        # execution deadline before its own sleep ended at 30 minutes.
        assert [r.status for r in runs] == [
            ExecutionStatus.CONTINUED_AS_NEW,
            ExecutionStatus.CONTINUED_AS_NEW,
            ExecutionStatus.TIMED_OUT,
        ]


async def test_workflow_retries_until_success(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.start_workflow(flaky, 3, id="flaky-1", retry_policy=RETRY)
        await eventually(lambda: store.run_count == 2)
        # The retry waits out its backoff (10s, then 20s).
        await clock.advance(10)
        await eventually(lambda: store.run_count == 3)
        await clock.advance(20)
        assert await handle.result(timeout=5) == "succeeded on attempt 3"
        runs = sorted(await store.list_executions(), key=lambda r: r.created_at)
        assert [r.status for r in runs].count(ExecutionStatus.FAILED) == 2
        assert all(r.continued_run_id for r in runs if r.status is ExecutionStatus.FAILED)
        assert (await store.find_execution("flaky-1")).status is ExecutionStatus.COMPLETED


async def test_retries_are_exhausted(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.start_workflow(flaky, 10, retry_policy=RETRY)
        for expected in (2, 3):
            await eventually(lambda expected=expected: store.run_count == expected)
            await clock.advance(60)
        with pytest.raises(lw.WorkflowFailedError, match="attempt 3 failed"):
            await handle.result(timeout=5)
        assert store.run_count == 3


async def test_non_retryable_failure_is_final(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.start_workflow(fatal, retry_policy=RETRY)
        with pytest.raises(lw.WorkflowFailedError, match="never retry"):
            await handle.result(timeout=5)
        assert store.run_count == 1


async def test_retry_policy_rejects_bad_timeouts(clock: ManualClock) -> None:
    async with make(clock, MemoryStore()) as app:
        with pytest.raises(ValueError, match="run_timeout"):
            await app.start_workflow(sleeper, 1, run_timeout=0)


async def test_child_failure_without_retry_policy(clock: ManualClock) -> None:
    async with make(clock, MemoryStore()) as app:
        assert await app.execute(parent, timeout=5) == "child failed"
