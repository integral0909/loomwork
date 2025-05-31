"""Long-polling behaviour of ``claim_tick``/``claim_step``: idle claims return promptly by
default, a positive ``wait`` holds the request open until work appears or the wait (capped
server-side) elapses, and a claim that arrives mid-wait is returned without waiting the
full duration.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import timedelta

import httpx
import pytest

from loomwork.remote.http_store import HttpStore
from loomwork.server.app import create_app
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.records import ExecutionRecord, ExecutionStatus, TickOutcome
from tests.helpers import T0

LEASE = timedelta(seconds=30)


@pytest.fixture
async def store() -> AsyncIterator[HttpStore]:
    config = ServerConfig(
        namespaces={"default": "memory://"},
        auth=AuthConfig(disabled=True),
    )
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        http_store = HttpStore("http://test", None, "default", transport=transport, long_poll=0)
        await http_store.open()
        try:
            yield http_store
        finally:
            await http_store.close()


async def _create(store: HttpStore, run_id: str = "r1") -> ExecutionRecord:
    from loomwork.codec import default_converter
    from loomwork.history.events import WorkflowStarted

    record = ExecutionRecord(
        run_id=run_id,
        workflow_id=f"wid-{run_id}",
        workflow_name="wf",
        status=ExecutionStatus.RUNNING,
        created_at=T0,
    )
    started = WorkflowStarted(
        workflow_name="wf",
        workflow_id=record.workflow_id,
        run_id=run_id,
        args=[default_converter.encode("x")],
    )
    created, _ = await store.create_execution(record, started, T0)
    return created


async def test_claim_tick_without_wait_returns_immediately(store: HttpStore) -> None:
    start = time.monotonic()
    claimed = await store.claim_tick("w1", T0, LEASE)
    assert claimed is None
    assert time.monotonic() - start < 0.5


async def test_claim_tick_long_poll_finds_work_created_during_the_wait(store: HttpStore) -> None:
    store.long_poll = 5.0

    async def create_soon() -> None:
        await asyncio.sleep(0.1)
        await _create(store, "r1")

    creator = asyncio.ensure_future(create_soon())
    start = time.monotonic()
    claimed = await store.claim_tick("w1", T0, LEASE)
    elapsed = time.monotonic() - start
    await creator

    assert claimed is not None
    assert claimed.run_id == "r1"
    # Woken by the notifier well before the full long-poll budget elapsed.
    assert elapsed < 2.0


async def test_claim_tick_long_poll_times_out_when_nothing_arrives(store: HttpStore) -> None:
    store.long_poll = 0.2
    start = time.monotonic()
    claimed = await store.claim_tick("w1", T0, LEASE)
    elapsed = time.monotonic() - start
    assert claimed is None
    assert elapsed >= 0.15


async def test_claim_step_long_poll_finds_work(store: HttpStore) -> None:
    from tests.store_conformance import job

    store.long_poll = 5.0
    await _create(store)
    await store.claim_tick("w1", T0, LEASE)

    async def schedule_soon() -> None:
        await asyncio.sleep(0.1)
        await store.complete_tick(
            "r1", "w1", TickOutcome("w1", 1, T0, new_steps=[job("r1", 1)]), T0
        )

    scheduler = asyncio.ensure_future(schedule_soon())
    start = time.monotonic()
    claimed = await store.claim_step("w2", T0, LEASE)
    elapsed = time.monotonic() - start
    await scheduler

    assert claimed is not None
    assert claimed.seq == 1
    assert elapsed < 2.0


async def test_server_caps_the_requested_wait(store: HttpStore) -> None:
    """A client asking for a longer wait than the server allows is capped, not honoured
    verbatim; the low server-side cap here keeps the test fast."""
    server = store._transport.app.state.server  # type: ignore[union-attr]
    server.config.limits.long_poll_max_seconds = 0.2
    store.long_poll = 10.0  # far more than the server will actually wait
    start = time.monotonic()
    claimed = await store.claim_tick("w1", T0, LEASE)
    elapsed = time.monotonic() - start
    assert claimed is None
    assert elapsed < 1.0
