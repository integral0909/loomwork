"""Schedules end to end: a worker's scheduler loop starts workflow runs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import loomwork as lw
from loomwork import ExecutionStatus, ManualClock, MemoryStore
from loomwork.errors import ScheduleExistsError
from loomwork.options import IdConflictPolicy, IdReusePolicy, VersioningPolicy
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


# --------------------------------------------------------------------------- id and versioning policies


async def test_schedule_versioning_policy_pins_every_started_run(clock: ManualClock) -> None:
    store = MemoryStore()
    app = lw.Loomwork(
        store=store,
        workflows=[daily_report],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2, build_id="build-a"),
    )
    async with app:
        reports.clear()
        handle = await app.client.schedules.create(
            "pinned-schedule",
            daily_report,
            "us",
            cron="0 * * * *",
            versioning_policy=VersioningPolicy.PINNED,
        )
        await clock.advance(timedelta(hours=1, seconds=1))
        await eventually(lambda: len(reports) == 1)
        runs = await runs_of(store, "sched.report")
        assert len(runs) == 1
        assert runs[0].versioning_policy is VersioningPolicy.PINNED
        assert runs[0].pinned_build_id == "build-a"
        assert (await handle.describe()).action.versioning_policy is VersioningPolicy.PINNED


async def test_schedule_run_timeout_applies_to_started_runs(clock: ManualClock) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        await app.client.schedules.create(
            "short-lived", slow_job, interval=3600, run_timeout=timedelta(minutes=30)
        )
        await clock.advance(timedelta(hours=1, seconds=1))
        await eventually(lambda: store.run_count == 1)
        await clock.advance(timedelta(minutes=31))

        async def timed_out() -> bool:
            runs = await runs_of(store, "sched.slow")
            return bool(runs) and runs[0].status is ExecutionStatus.TIMED_OUT

        await eventually(timed_out)


async def test_schedule_id_conflict_use_existing_adopts_repeated_trigger(
    clock: ManualClock,
) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.client.schedules.create(
            "attach-test",
            slow_job,
            interval=3600,
            id_conflict_policy=IdConflictPolicy.USE_EXISTING,
        )
        await handle.trigger()
        await eventually(lambda: store.run_count == 1)
        first_run_id = (await handle.describe()).running[0].run_id

        # A second trigger at the same nominal time (the clock has not moved) targets
        # the identical workflow id; USE_EXISTING adopts the run already going instead
        # of failing or creating a second one.
        await handle.trigger()

        async def fired_twice() -> bool:
            return (await handle.describe()).total_started == 2

        await eventually(fired_twice)
        state = await handle.describe()
        assert len(state.running) == 1
        assert state.running[0].run_id == first_run_id
        assert store.run_count == 1


async def test_schedule_id_conflict_terminate_existing_replaces_repeated_trigger(
    clock: ManualClock,
) -> None:
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.client.schedules.create(
            "replace-test",
            slow_job,
            interval=3600,
            id_conflict_policy=IdConflictPolicy.TERMINATE_EXISTING,
        )
        await handle.trigger()
        await eventually(lambda: store.run_count == 1)
        first_run_id = (await handle.describe()).running[0].run_id

        await handle.trigger()

        async def fired_twice() -> bool:
            return (await handle.describe()).total_started == 2

        await eventually(fired_twice)
        state = await handle.describe()
        assert len(state.running) == 1
        assert state.running[0].run_id != first_run_id
        old = await app.store.get_execution(first_run_id)
        assert old.status is ExecutionStatus.TERMINATED


async def test_schedule_id_reuse_policy_persists_and_loads_on_old_documents(
    clock: ManualClock,
) -> None:
    """A schedule created before id_reuse_policy/id_conflict_policy/versioning_policy/
    run_timeout/execution_timeout/search_attributes existed has none of them in its
    stored document; loading it must not fail, and it must keep behaving as it always
    did (see ScheduleAction.from_dict)."""
    store = MemoryStore()
    async with make(clock, store) as app:
        handle = await app.client.schedules.create(
            "plain", slow_job, interval=3600, id_reuse_policy=IdReusePolicy.REJECT_DUPLICATE
        )
        state = await handle.describe()
        assert state.action.id_reuse_policy is IdReusePolicy.REJECT_DUPLICATE
        # Simulate a document stored before these fields existed by dropping them from
        # the persisted body and reloading through the same store.
        stale_dict = state.to_dict()
        del stale_dict["action"]["id_reuse_policy"]
        from loomwork.schedules.model import ScheduleState

        stale = ScheduleState.from_dict(stale_dict)
        assert stale.action.id_reuse_policy is None
        await store.update_schedule(stale, state.version, clock.now())
        reloaded = await handle.describe()
        assert reloaded.action.id_reuse_policy is None
        assert reloaded.action.id_conflict_policy is None
