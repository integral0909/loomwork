"""IdReusePolicy / IdConflictPolicy: behaviour with real workers, and concurrency races
directly against the durable stores (where two callers can genuinely interleave)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock
from loomwork.codec import default_converter
from loomwork.errors import WorkflowAlreadyStartedError, WorkflowIdReuseRejectedError
from loomwork.history.events import WorkflowStarted
from loomwork.options import IdConflictPolicy, IdReusePolicy
from loomwork.store.base import Store
from loomwork.store.records import ExecutionRecord
from loomwork.store.sqlite import SQLiteStore
from tests.conftest import AppFactory
from tests.helpers import T0
from tests.postgres_support import fresh_postgres_store, requires_postgres

pytestmark = pytest.mark.integration


@lw.step(name="idp.charge")
async def charge(order_id: str) -> str:
    return f"charged:{order_id}"


@lw.workflow(name="idp.slow")
async def slow_order(order_id: str, seconds: float) -> str:
    await lw.sleep(timedelta(seconds=seconds))
    return await charge(order_id)


@lw.workflow(name="idp.fast")
async def fast_order(order_id: str) -> str:
    return await charge(order_id)


@lw.workflow(name="idp.broken")
async def broken_order(order_id: str) -> str:
    raise lw.ApplicationError(f"cannot charge {order_id}")


WORKFLOWS = [slow_order, fast_order, broken_order]
STEPS = [charge]


async def test_use_existing_returns_a_handle_to_the_running_run(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    first = await app.start_workflow(slow_order, "A-1", 3600, id="order-A-1")
    second = await app.client.start(
        slow_order,
        "A-1",
        3600,
        id="order-A-1",
        id_conflict_policy=IdConflictPolicy.USE_EXISTING,
    )
    assert second.run_id == first.run_id
    assert len(await app.client.list()) == 1


async def test_terminate_existing_replaces_the_running_run(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    first = await app.start_workflow(slow_order, "A-1", 3600, id="order-A-1")
    second = await app.client.start(
        fast_order, "A-1", id="order-A-1", id_conflict_policy=IdConflictPolicy.TERMINATE_EXISTING
    )
    assert second.run_id != first.run_id
    old_record = await first.describe()
    assert old_record.status is ExecutionStatus.TERMINATED
    assert old_record.close_reason == "superseded by id conflict policy"
    assert await second.result(timeout=5) == "charged:A-1"


async def test_fail_is_the_default_conflict_policy(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    await app.start_workflow(slow_order, "A-1", 3600, id="order-A-1")
    with pytest.raises(WorkflowAlreadyStartedError):
        await app.start_workflow(fast_order, "A-1", id="order-A-1")


async def test_reject_duplicate_after_success(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app(WORKFLOWS, STEPS)
    handle = await app.start_workflow(fast_order, "A-1", id="order-A-1")
    assert await handle.result(timeout=5) == "charged:A-1"
    with pytest.raises(WorkflowIdReuseRejectedError):
        await app.client.start(
            fast_order, "A-1", id="order-A-1", id_reuse_policy=IdReusePolicy.REJECT_DUPLICATE
        )


async def test_allow_duplicate_failed_only(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app(WORKFLOWS, STEPS)

    failed = await app.start_workflow(broken_order, "A-1", id="order-broken")
    with pytest.raises(lw.WorkflowFailedError):
        await failed.result(timeout=5)
    retried = await app.client.start(
        fast_order,
        "A-1",
        id="order-broken",
        id_reuse_policy=IdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
    )
    assert await retried.result(timeout=5) == "charged:A-1"

    succeeded = await app.start_workflow(fast_order, "A-2", id="order-ok")
    assert await succeeded.result(timeout=5) == "charged:A-2"
    with pytest.raises(WorkflowIdReuseRejectedError):
        await app.client.start(
            fast_order,
            "A-2",
            id="order-ok",
            id_reuse_policy=IdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
        )


# --------------------------------------------------------------------------- races
#
# These exercise the store directly (rather than through the engine) with genuinely
# concurrent callers against a durable backend, where -- unlike MemoryStore, whose
# methods never await mid-operation -- two ``asyncio`` tasks can actually interleave
# inside the database driver.

N_RACERS = 8


async def _start(store: Store, workflow_id: str, i: int, **kwargs: Any) -> str | None:
    run_id = f"race-{workflow_id}-{i}"
    record = ExecutionRecord(
        run_id=run_id,
        workflow_id=workflow_id,
        workflow_name="race",
        status=ExecutionStatus.RUNNING,
        created_at=T0,
    )
    started = WorkflowStarted(
        workflow_name="race",
        workflow_id=workflow_id,
        run_id=run_id,
        args=default_converter.encode_args(["x"]),
    )
    try:
        created, _ = await store.create_execution(record, started, T0, **kwargs)
        return created.run_id
    except WorkflowAlreadyStartedError:
        return None


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=requires_postgres)])
async def durable_store(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Store]:
    if request.param == "sqlite":
        sqlite_store = SQLiteStore(tmp_path / "race.db")
        await sqlite_store.open()
        try:
            yield sqlite_store
        finally:
            await sqlite_store.close()
    else:
        async with fresh_postgres_store() as postgres_store:
            yield postgres_store


async def test_concurrent_start_fail_policy_admits_exactly_one(durable_store: Store) -> None:
    results = await asyncio.gather(*(_start(durable_store, "wf-fail", i) for i in range(N_RACERS)))
    assert sum(r is not None for r in results) == 1


async def test_concurrent_start_use_existing_all_agree_on_one_run(durable_store: Store) -> None:
    results = await asyncio.gather(
        *(
            _start(durable_store, "wf-use", i, id_conflict_policy=IdConflictPolicy.USE_EXISTING)
            for i in range(N_RACERS)
        )
    )
    assert all(r is not None for r in results)
    assert len(set(results)) == 1
    rows = await durable_store.list_executions(limit=1000)
    assert len([r for r in rows if r.workflow_id == "wf-use"]) == 1


async def test_concurrent_start_terminate_existing_ends_with_one_running(
    durable_store: Store,
) -> None:
    await asyncio.gather(
        *(
            _start(
                durable_store, "wf-term", i, id_conflict_policy=IdConflictPolicy.TERMINATE_EXISTING
            )
            for i in range(N_RACERS)
        )
    )
    rows = [
        r for r in await durable_store.list_executions(limit=1000) if r.workflow_id == "wf-term"
    ]
    assert len(rows) == N_RACERS  # every attempt created its own run
    running = [r for r in rows if r.status is ExecutionStatus.RUNNING]
    terminated = [r for r in rows if r.status is ExecutionStatus.TERMINATED]
    assert len(running) == 1
    assert len(terminated) == N_RACERS - 1
