"""Schedules end to end: a worker's scheduler loop starts workflow runs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock, MemoryStore
from loomwork.errors import ScheduleExistsError
from loomwork.schedules.model import OverlapPolicy
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, T0, eventually

pytestmark = pytest.mark.integration

reports: list[dict[str, Any]] = []


@lw.workflow(name="sched.report")
async def daily_report(region: str) -> str:
    info = lw.info()
    reports.append({"region": region, "nominal": info.scheduled_time, "schedule": info.schedule_id})
    return f"report for {region}"


@lw.workflow(name="sched.slow")
async def slow_job() -> str:
    await lw.sleep(timedelta(hours=3))
    return "done"


@lw.workflow(name="sched.flaky")
async def flaky_job() -> None:
    raise lw.ApplicationError("broken", type="Broken")


def make(clock: ManualClock, store: MemoryStore) -> lw.Loomwork:
    return lw.Loomwork(
        store=store,
        workflows=[daily_report, slow_job, flaky_job],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )


async def runs_of(store: MemoryStore, name: str) -> list[lw.ExecutionRecord]:
    return await store.list_executions(workflow_name=name)


async def test_cron_schedule_starts_runs(clock: ManualClock) -> None:
    reports.clear()
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.client.schedules.create(
            "reports-emea", daily_report, "emea", cron="0 6 * * *", timezone="Europe/Berlin"
        )
        assert [t.isoformat() for t in await handle.upcoming(2)] == [
            "2024-01-01T05:00:00+00:00",
            "2024-01-02T05:00:00+00:00",
        ]
        await clock.set(datetime(2024, 1, 1, 5, 0, 1, tzinfo=UTC))
        await eventually(lambda: len(reports) == 1)
        await clock.set(datetime(2024, 1, 2, 5, 0, 1, tzinfo=UTC))
        await eventually(lambda: len(reports) == 2)
        assert reports[0] == {
            "region": "emea",
            "nominal": datetime(2024, 1, 1, 5, tzinfo=UTC),
            "schedule": "reports-emea",
        }
        state = await handle.describe()
        assert state.total_started == 2
        runs = await runs_of(store, "sched.report")
        assert {r.workflow_id for r in runs} == {
            "reports-emea@2024-01-01T05:00:00Z",
            "reports-emea@2024-01-02T05:00:00Z",
        }
        with pytest.raises(ScheduleExistsError):
            await app.client.schedules.create("reports-emea", daily_report, "x", cron="* * * * *")


async def test_overlap_skip_and_terminate_other(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        skip = await app.client.schedules.create("skipper", slow_job, interval=3600)
        term = await app.client.schedules.create(
            "terminator", slow_job, interval=3600, overlap=OverlapPolicy.TERMINATE_OTHER
        )
        await clock.advance(timedelta(hours=1, seconds=1))
        await eventually(lambda: store.run_count == 2)
        await clock.advance(timedelta(hours=1))

        async def settled() -> bool:
            if (await term.describe()).total_started != 2:
                return False
            if (await skip.describe()).total_skipped < 1:
                return False
            statuses = [r.status for r in await runs_of(store, "sched.slow")]
            return statuses.count(ExecutionStatus.TERMINATED) == 1

        await eventually(settled)
        by_id = {r.workflow_id: r.status for r in await runs_of(store, "sched.slow")}
        assert by_id == {
            "skipper@2024-01-01T01:00:00Z": ExecutionStatus.RUNNING,
            "terminator@2024-01-01T01:00:00Z": ExecutionStatus.TERMINATED,
            "terminator@2024-01-01T02:00:00Z": ExecutionStatus.RUNNING,
        }


async def test_pause_unpause_trigger_backfill(clock: ManualClock) -> None:
    reports.clear()
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.client.schedules.create("hourly", daily_report, "us", cron="0 * * * *")
        await handle.pause("maintenance")
        await clock.advance(timedelta(hours=3))
        await eventually(lambda: store.run_count == 0)
        state = await handle.describe()
        assert state.paused and state.note == "maintenance"
        await handle.unpause()
        await handle.trigger()
        await eventually(lambda: len(reports) == 1)
        await handle.backfill(T0 - timedelta(hours=3), T0)
        await eventually(lambda: len(reports) == 4)
        assert (await handle.describe()).total_started == 4
        assert await handle.delete()
        assert await app.client.schedules.list() == []


async def test_pause_on_failure(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.client.schedules.create(
            "fragile", flaky_job, interval=60, pause_on_failure=True
        )
        await clock.advance(61)

        async def failed() -> bool:
            runs = await runs_of(store, "sched.flaky")
            return bool(runs) and runs[0].status is ExecutionStatus.FAILED

        await eventually(failed)
        await clock.advance(60)

        async def paused() -> bool:
            return (await handle.describe()).paused

        await eventually(paused)
        assert "failed" in (await handle.describe()).note
