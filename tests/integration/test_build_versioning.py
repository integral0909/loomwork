"""Two builds of the same workflow name, running side by side against a real store.

``old_flow`` and ``new_flow`` are registered under the identical workflow name
(``versioned.flow``) but schedule a different final step -- "calling a different step
at the same point" is explicitly unsafe to replay (see ``docs/guides/versioning.md``).
Without build pinning this would be a textbook nondeterminism bug the moment a new
worker picked up an old run. With ``VersioningPolicy.PINNED`` and a build id per worker
(``docs/adr/0006-worker-build-versioning.md``), a run started while only the old build
is running stays on it even after the new build joins the fleet with its incompatible
code, and a run started once only the new build remains lands on the new code -- with no
``NondeterminismError`` anywhere.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

import loomwork as lw
from loomwork import ManualClock
from loomwork.codec import default_converter
from loomwork.engine.engine import Engine
from loomwork.history.events import SignalReceived
from loomwork.options import VersioningPolicy
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.records import ExecutionStatus
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker.worker import Worker, WorkerOptions
from tests.helpers import FAST_CONFIG, eventually
from tests.postgres_support import DSN, fresh_postgres_store

pytestmark = pytest.mark.integration

# --------------------------------------------------------------------------- workflow code

go = lw.Channel("bv.go", str)


@lw.step(name="bv.step_a")
async def step_a(key: str) -> str:
    return f"a:{key}"


@lw.step(name="bv.old_finish")
async def old_finish(key: str) -> str:
    return f"old:{key}"


@lw.step(name="bv.new_finish")
async def new_finish(key: str) -> str:
    return f"new:{key}"


@lw.workflow(name="versioned.flow")
async def old_flow(key: str) -> str:
    await step_a(key)
    reason = await go.receive()
    return await old_finish(f"{key}-{reason}")


@lw.workflow(name="versioned.flow")
async def new_flow(key: str) -> str:
    await step_a(key)
    reason = await go.receive()
    return await new_finish(f"{key}-{reason}")


OLD_REGISTRY = Registry([old_flow], [step_a, old_finish])
NEW_REGISTRY = Registry([new_flow], [step_a, new_finish])

# --------------------------------------------------------------------------- fixtures


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)])
def backend(request: pytest.FixtureRequest) -> str:
    if request.param == "postgres" and not DSN:
        pytest.skip("set LOOMWORK_TEST_POSTGRES_DSN to run PostgreSQL tests")
    return str(request.param)


@pytest.fixture
async def store(backend: str, tmp_path: Path) -> AsyncIterator[Store]:
    if backend == "sqlite":
        sqlite_store = SQLiteStore(tmp_path / "build-versioning.db")
        await sqlite_store.open()
        try:
            yield sqlite_store
        finally:
            await sqlite_store.close()
    else:
        async with fresh_postgres_store() as pg:
            yield pg


def worker_on(store: Store, registry: Registry, clock: ManualClock, build_id: str) -> Worker:
    return Worker(
        store,
        registry,
        clock=clock,
        config=FAST_CONFIG,
        options=WorkerOptions(build_id=build_id, shutdown_grace=0.2),
    )


async def start_pinned(engine: Engine, workflow_id: str, key: str) -> Any:
    return await engine.start_workflow(
        "versioned.flow",
        [default_converter.encode(key)],
        workflow_id=workflow_id,
        versioning_policy=VersioningPolicy.PINNED,
    )


async def send_go(engine: Engine, run_id: str, reason: str) -> None:
    event = SignalReceived(channel=go.name, payload=default_converter.encode(reason))
    assert await engine.store.append_input(run_id, event, engine.clock.now()) is not None


# --------------------------------------------------------------------------- the test


async def test_old_run_survives_a_new_incompatible_build_joining(
    store: Store, clock: ManualClock
) -> None:
    engine = Engine(store, clock=clock, config=FAST_CONFIG)
    old_worker = worker_on(store, OLD_REGISTRY, clock, "old")
    new_worker = worker_on(store, NEW_REGISTRY, clock, "new")
    await old_worker.start()
    try:
        old_run = await start_pinned(engine, "bv-old-run", "k1")

        # The old build's only worker processes the first tick and pins the run to it.
        await eventually(
            lambda: _pinned_to(engine, old_run.run_id, "old"), clock=clock, advance=0.05
        )
        await eventually(
            lambda: _has_no_pending_steps(engine, old_run.run_id), clock=clock, advance=0.05
        )

        # The new build joins the fleet with an *incompatible* implementation of the
        # same workflow name, while the old run is still in flight (blocked on `go`).
        await new_worker.start()
        await send_go(engine, old_run.run_id, "old-path")
        old_record = await engine.wait_closed(old_run.run_id, timeout=5)
        assert old_record.status is ExecutionStatus.COMPLETED
        assert old_record.tick_failures == 0
        assert old_record.pinned_build_id == "old"
        result = default_converter.decode(old_record.result) if old_record.result else None
        assert result == "old:k1-old-path"  # ran old_finish, never new_finish

        # Retire the old build entirely; a fresh run must now land on the new one.
        await old_worker.stop()

        new_run = await start_pinned(engine, "bv-new-run", "k2")
        await eventually(
            lambda: _pinned_to(engine, new_run.run_id, "new"), clock=clock, advance=0.05
        )
        await eventually(
            lambda: _has_no_pending_steps(engine, new_run.run_id), clock=clock, advance=0.05
        )
        await send_go(engine, new_run.run_id, "new-path")
        new_record = await engine.wait_closed(new_run.run_id, timeout=5)
        assert new_record.status is ExecutionStatus.COMPLETED
        assert new_record.tick_failures == 0
        assert new_record.pinned_build_id == "new"
        result = default_converter.decode(new_record.result) if new_record.result else None
        assert result == "new:k2-new-path"

        assert await store.count_pinned_runs() == {}  # both runs closed
    finally:
        if old_worker.running:
            await old_worker.stop()
        if new_worker.running:
            await new_worker.stop()


async def _pinned_to(engine: Engine, run_id: str, build_id: str) -> bool:
    record = await engine.describe(run_id)
    return record.pinned_build_id == build_id


async def _has_no_pending_steps(engine: Engine, run_id: str) -> bool:
    return not await engine.pending_steps(run_id)
