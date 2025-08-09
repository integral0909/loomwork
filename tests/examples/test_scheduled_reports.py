"""Tests for ``examples.scheduled_reports``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from examples.scheduled_reports import steps
from examples.scheduled_reports.workflows import generate_report
from examples.support import advance_until
from loomwork.errors import ScheduleExistsError
from loomwork.schedules.model import OverlapPolicy

if TYPE_CHECKING:
    from loomwork import ManualClock
    from tests.conftest import AppFactory


@pytest.fixture(autouse=True)
def _reset_archive() -> None:
    steps.archive.generated.clear()


async def test_cron_fires_at_the_expected_utc_instant_for_its_time_zone(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([generate_report], [steps.build_report])
    handle = await app.client.schedules.create(
        "daily-eu", generate_report, "daily", "eu", cron="0 6 * * *", timezone="Europe/Berlin"
    )

    upcoming = await handle.upcoming(2)
    assert [t.isoformat() for t in upcoming] == [
        "2024-01-01T05:00:00+00:00",
        "2024-01-02T05:00:00+00:00",
    ]

    await advance_until(clock, lambda: len(steps.archive.generated) >= 1, step=timedelta(hours=1))
    report = steps.archive.generated[0]
    assert report.nominal_time == datetime(2024, 1, 1, 5, tzinfo=UTC)
    assert report.report_id == "daily-eu-2024-01-01"


async def test_pause_then_backfill_catches_up_the_missed_window(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([generate_report], [steps.build_report])
    handle = await app.client.schedules.create(
        "daily-eu",
        generate_report,
        "daily",
        "eu",
        cron="0 6 * * *",
        timezone="Europe/Berlin",
        catchup_window=timedelta(hours=6),
    )
    await advance_until(clock, lambda: len(steps.archive.generated) >= 1, step=timedelta(hours=1))

    pause_started_at = clock.now()
    await handle.pause("maintenance")
    await clock.advance(timedelta(days=3))
    state = await handle.describe()
    assert state.paused
    assert state.total_started == 1  # nothing fired while paused

    await handle.unpause()
    await handle.backfill(pause_started_at, clock.now())
    await advance_until(clock, lambda: len(steps.archive.generated) >= 4, step=timedelta(hours=1))
    state = await handle.describe()
    assert state.total_started == 4
    assert [r.report_id for r in steps.archive.generated] == [
        "daily-eu-2024-01-01",
        "daily-eu-2024-01-02",
        "daily-eu-2024-01-03",
        "daily-eu-2024-01-04",
    ]


async def test_overlapping_fires_are_skipped_until_the_slow_run_finishes(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([generate_report], [steps.build_report])
    handle = await app.client.schedules.create(
        "audit-hourly",
        generate_report,
        "audit",
        "global",
        180.0,
        interval=timedelta(hours=1),
        overlap=OverlapPolicy.SKIP,
    )

    async def started(n: int) -> bool:
        return (await handle.describe()).total_started >= n

    async def skipped(n: int) -> bool:
        return (await handle.describe()).total_skipped >= n

    await advance_until(clock, lambda: started(1), step=timedelta(minutes=15))
    await advance_until(clock, lambda: skipped(1), step=timedelta(hours=1))
    mid_state = await handle.describe()
    assert mid_state.total_started == 1
    assert mid_state.total_skipped >= 1

    await advance_until(
        clock, lambda: len(steps.archive.generated) >= 2, step=timedelta(hours=1), timeout=15
    )
    final_state = await handle.describe()
    assert final_state.total_started == 2
    assert len(steps.archive.generated) == 2


async def test_creating_a_schedule_with_a_taken_id_fails(make_app: AppFactory) -> None:
    app = await make_app([generate_report], [steps.build_report])
    await app.client.schedules.create("daily-eu", generate_report, "daily", "eu", cron="0 6 * * *")
    with pytest.raises(ScheduleExistsError):
        await app.client.schedules.create(
            "daily-eu", generate_report, "daily", "elsewhere", cron="0 7 * * *"
        )
