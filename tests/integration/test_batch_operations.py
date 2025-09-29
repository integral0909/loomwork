"""Client.batch_signal / batch_cancel / batch_terminate: matching, paging, bounded
concurrency, and per-run failure isolation."""

from __future__ import annotations

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock
from tests.conftest import AppFactory

pytestmark = pytest.mark.integration


@lw.workflow(name="batch.waits")
async def waits_forever() -> str:
    cancel = lw.Channel("cancel", str)
    return await cancel.receive()


WORKFLOWS = [waits_forever]


async def test_batch_terminate_matches_and_applies(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, [])
    handles = [await app.start_workflow(waits_forever, id=f"batch-{i}") for i in range(5)]

    result = await app.client.batch_terminate("status = 'running'", reason="cleanup")

    assert result.matched == 5
    assert result.succeeded == 5
    assert result.skipped_closed == 0
    assert result.failed == ()
    for handle in handles:
        record = await handle.describe()
        assert record.status is ExecutionStatus.TERMINATED
        assert record.close_reason == "cleanup"


async def test_batch_signal_delivers_to_every_match(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, [])
    handles = [await app.start_workflow(waits_forever, id=f"sig-{i}") for i in range(4)]

    result = await app.client.batch_signal("status = 'running'", "cancel", "batch reason")

    assert result.matched == 4
    assert result.succeeded == 4
    for handle in handles:
        assert await handle.result(timeout=5) == "batch reason"


async def test_batch_terminate_counts_already_closed_as_skipped(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app(WORKFLOWS, [])
    handles = [await app.start_workflow(waits_forever, id=f"skip-{i}") for i in range(4)]
    await handles[0].terminate("already done")
    await handles[1].terminate("already done")

    result = await app.client.batch_terminate("workflow_name = 'batch.waits'", reason="cleanup")

    assert result.matched == 4
    assert result.succeeded == 2
    assert result.skipped_closed == 2
    assert result.failed == ()


async def test_batch_pages_through_many_matches(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app(WORKFLOWS, [])
    for i in range(25):
        await app.start_workflow(waits_forever, id=f"page-{i}")

    result = await app.client.batch_terminate("status = 'running'", page_size=5, concurrency=3)

    assert result.matched == 25
    assert result.succeeded == 25
    assert (await app.client.list(status=ExecutionStatus.RUNNING)) == []


async def test_batch_isolates_individual_failures(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app(WORKFLOWS, [])
    handles = [await app.start_workflow(waits_forever, id=f"fail-{i}") for i in range(3)]
    broken_run_id = handles[1].run_id
    original_terminate = app.engine.terminate

    async def flaky_terminate(run_id: str, reason: str = "") -> bool:
        if run_id == broken_run_id:
            raise RuntimeError("kaboom")
        return await original_terminate(run_id, reason)

    app.engine.terminate = flaky_terminate  # type: ignore[method-assign]

    result = await app.client.batch_terminate("status = 'running'", reason="x")

    assert result.matched == 3
    assert result.succeeded == 2
    assert len(result.failed) == 1
    assert result.failed[0].run_id == broken_run_id
    assert "kaboom" in result.failed[0].error
    # The broken run is untouched, but the other two matches still went through despite it.
    assert (await handles[1].describe()).status is ExecutionStatus.RUNNING
