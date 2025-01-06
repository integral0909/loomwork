"""Crash recovery and multi-worker behaviour on the durable stores."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock
from loomwork.store.base import Store
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, T0, eventually
from tests.postgres_support import DSN, fresh_postgres_store

pytestmark = pytest.mark.integration

StoreFactory = Callable[[], Any]

executions: dict[str, int] = {}
release = asyncio.Event()


@lw.step(
    name="durable.record", retry=lw.RetryPolicy(initial_interval=0.01, max_attempts=5, jitter=0)
)
async def record_execution(key: str) -> str:
    executions[key] = executions.get(key, 0) + 1
    return key


@lw.step(
    name="durable.block",
    timeout=0.2,
    retry=lw.RetryPolicy(initial_interval=0.01, max_attempts=5, jitter=0),
)
async def block_until_released(key: str) -> str:
    executions[key] = executions.get(key, 0) + 1
    if executions[key] == 1:
        # First attempt: hang as if the worker process froze.
        await asyncio.sleep(3600)
    return f"done:{key}"


@lw.workflow(name="durable.chain")
async def chain(key: str, length: int) -> list[str]:
    results = []
    for i in range(length):
        results.append(await record_execution(f"{key}.{i}"))
        await lw.sleep(timedelta(minutes=10))
    return results


@lw.workflow(name="durable.blocking")
async def blocking(key: str) -> str:
    return await block_until_released(key)


@lw.workflow(name="durable.parallel")
async def parallel(key: str, n: int) -> int:
    done = await asyncio.gather(*(record_execution(f"{key}.{i}") for i in range(n)))
    return len(done)


STEPS: list[lw.StepDefinition[..., Any]] = [record_execution, block_until_released]
WORKFLOWS: list[lw.WorkflowDefinition[..., Any]] = [chain, blocking, parallel]


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)])
def backend(request: pytest.FixtureRequest) -> str:
    if request.param == "postgres" and not DSN:
        pytest.skip("set LOOMWORK_TEST_POSTGRES_DSN to run PostgreSQL tests")
    return str(request.param)


@pytest.fixture
def open_store(backend: str, tmp_path: Path) -> Callable[[], Any]:
    """Opens a *new store instance* on the same database each time it is called,
    the way a restarted process would."""
    first = [True]

    @asynccontextmanager
    async def factory() -> AsyncIterator[Store]:
        if backend == "sqlite":
            store = SQLiteStore(tmp_path / "durable.db")
            await store.open()
            try:
                yield store
            finally:
                await store.close()
        elif first[0]:
            first[0] = False
            async with fresh_postgres_store() as pg:
                yield pg
        else:
            from loomwork.store.postgres import PostgresStore

            async with PostgresStore(DSN) as pg:
                yield pg

    return factory


def app_on(store: Store, clock: ManualClock, **kwargs: Any) -> lw.Loomwork:
    kwargs.setdefault("config", FAST_CONFIG)
    kwargs.setdefault("worker_options", WorkerOptions(shutdown_grace=0.2))
    return lw.Loomwork(store=store, workflows=WORKFLOWS, steps=STEPS, clock=clock, **kwargs)


async def test_run_survives_process_restarts(open_store: StoreFactory, clock: ManualClock) -> None:
    executions.clear()
    # Process 1 starts the run and gets through the first step.
    async with open_store() as store:
        app = app_on(store, clock)
        async with app:
            handle = await app.start_workflow(chain, "restart", 3, id="chain-restart")
            await eventually(lambda: executions.get("restart.0") == 1)
            await eventually(lambda: store.next_timer_at(), timeout=5)
        run_id = handle.run_id

    # Process 2 sees the timer due, fires it, runs the second step.
    await clock.advance(timedelta(minutes=10))
    async with open_store() as store:
        app = app_on(store, clock)
        async with app:
            await eventually(lambda: executions.get("restart.1") == 1)

    # Process 3 finishes the run.
    async with open_store() as store:
        app = app_on(store, clock)
        async with app:
            handle = app.get_handle(run_id=run_id)
            result_task = asyncio.ensure_future(handle.result(timeout=10))
            for _ in range(20):
                await clock.advance(timedelta(minutes=10))
                await asyncio.sleep(0.05)
                if result_task.done():
                    break
            assert await result_task == ["restart.0", "restart.1", "restart.2"]
    # Every step ran exactly once despite two restarts.
    assert executions == {"restart.0": 1, "restart.1": 1, "restart.2": 1}


async def test_hung_step_attempt_is_retried_after_lease_expiry(
    open_store: StoreFactory, clock: ManualClock
) -> None:
    executions.clear()
    config = replace(FAST_CONFIG, step_lease_grace=timedelta(seconds=1))
    async with open_store() as store:
        # A "frozen" worker claims the step and never reports back.
        frozen = app_on(store, clock, config=config)
        async with frozen:
            handle = await frozen.start_workflow(blocking, "hung")
            await eventually(lambda: executions.get("hung") == 1)
            await frozen.worker.stop(grace=0)

        # The lease (timeout 0.2s + grace 1s) expires; a healthy worker retries.
        await clock.advance(timedelta(seconds=5))
        healthy = app_on(store, clock, config=config)
        async with healthy:
            assert await healthy.get_handle(run_id=handle.run_id).result(timeout=10) == "done:hung"
        assert executions["hung"] == 2


async def test_competing_workers_complete_each_run_once(
    open_store: StoreFactory, clock: ManualClock
) -> None:
    executions.clear()
    async with open_store() as store:
        workers = [app_on(store, clock, worker_id=f"worker-{i}") for i in range(3)]
        for w in workers:
            await w.start()
        try:
            handles = [await workers[i % 3].start_workflow(parallel, f"c{i}", 4) for i in range(15)]
            results = await asyncio.gather(*(h.result(timeout=30) for h in handles))
        finally:
            for w in workers:
                await w.stop(grace=0.5)
        assert results == [4] * 15
        assert len(executions) == 60
        assert set(executions.values()) == {1}
        ticks_by_worker = [w.worker.ticks.ticks_completed for w in workers]
        assert sum(ticks_by_worker) >= 15 * 2
        running = await store.list_executions(status=ExecutionStatus.RUNNING)
        assert running == []


async def test_history_is_identical_after_reload(
    open_store: StoreFactory, clock: ManualClock
) -> None:
    executions.clear()
    async with open_store() as store:
        app = app_on(store, clock)
        async with app:
            handle = await app.start_workflow(parallel, "reload", 3)
            await handle.result(timeout=10)
            original = await handle.history()
    async with open_store() as store:
        reloaded = await store.load_history(handle.run_id or "")
    assert reloaded == original
    assert reloaded[0].timestamp.tzinfo is not None
    assert reloaded[0].timestamp >= T0
