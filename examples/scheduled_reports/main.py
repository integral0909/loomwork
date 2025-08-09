"""Run three schedules: a daily report, a weekly report and an hourly audit whose
reports take longer to generate than the schedule's own interval.

.. code-block:: console

    uv run python -m examples.scheduled_reports.main

See ``README.md`` for what each schedule demonstrates.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import loomwork as lw
from examples.scheduled_reports.steps import archive, build_report
from examples.scheduled_reports.workflows import generate_report
from examples.support import DEMO_POLL_INTERVAL, advance_until, rule
from loomwork import ManualClock, MemoryStore
from loomwork.engine.config import EngineConfig
from loomwork.schedules.model import OverlapPolicy
from loomwork.worker import WorkerOptions


def _kinds_seen() -> set[str]:
    return {report.kind for report in archive.generated}


def _count(kind: str) -> int:
    return sum(1 for report in archive.generated if report.kind == kind)


async def main() -> None:
    clock = ManualClock()  # starts 2024-01-01T00:00:00Z, a Monday
    app = lw.Loomwork(
        store=MemoryStore(),
        workflows=[generate_report],
        steps=[build_report],
        clock=clock,
        config=EngineConfig(poll_interval=DEMO_POLL_INTERVAL),
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with app:
        print(rule("Creating a daily and a weekly schedule, each in its own time zone"))
        # This demo jumps the manual clock forward in large steps (see examples.support.
        # advance_until) rather than advancing it second by second, so a schedule's
        # evaluation can lag its nominal fire time by more simulated time than a real,
        # continuously-running deployment ever would. A wider catchup_window keeps a fire
        # time from being treated as a missed one purely because of that -- unrelated to
        # the overlap policy demonstrated by the audit schedule below.
        daily = await app.client.schedules.create(
            "daily-eu",
            generate_report,
            "daily",
            "eu",
            cron="0 6 * * *",
            timezone="Europe/Berlin",
            catchup_window=timedelta(hours=6),
        )
        weekly = await app.client.schedules.create(
            "weekly-us",
            generate_report,
            "weekly",
            "us",
            cron="0 8 * * MON",
            timezone="America/New_York",
            catchup_window=timedelta(hours=6),
        )
        print(f"daily-eu next fires (UTC): {[t.isoformat() for t in await daily.upcoming(2)]}")
        print(f"weekly-us next fires (UTC): {[t.isoformat() for t in await weekly.upcoming(2)]}")

        print(rule("Letting each schedule fire once"))
        await advance_until(
            clock, lambda: {"daily", "weekly"} <= _kinds_seen(), step=timedelta(hours=1)
        )
        for report in archive.generated:
            print(f"  generated {report.report_id} (nominal {report.nominal_time})")

        print(rule("Pausing the daily schedule, then catching up with a backfill"))
        daily_before_pause = _count("daily")
        pause_started_at = clock.now()
        await daily.pause("quarterly maintenance window")
        await clock.advance(timedelta(days=3))
        await asyncio.sleep(0.05)  # nothing should happen: the schedule is paused
        paused_state = await daily.describe()
        print(f"daily-eu total_started after 3 paused days: {paused_state.total_started}")
        await daily.unpause()
        await daily.backfill(pause_started_at, clock.now())
        await advance_until(
            clock, lambda: _count("daily") >= daily_before_pause + 3, step=timedelta(hours=1)
        )
        resumed_state = await daily.describe()
        print(
            "daily-eu total_started after backfilling the paused window: "
            f"{resumed_state.total_started}"
        )

        print(rule("Hourly audit: reports take 3 hours, but fire every hour (overlap=SKIP)"))
        audit = await app.client.schedules.create(
            "audit-hourly",
            generate_report,
            "audit",
            "global",
            180.0,
            interval=timedelta(hours=1),
            overlap=OverlapPolicy.SKIP,
        )

        async def audit_started(n: int) -> bool:
            return (await audit.describe()).total_started >= n

        async def audit_skipped(n: int) -> bool:
            return (await audit.describe()).total_skipped >= n

        await advance_until(clock, lambda: audit_started(1), step=timedelta(minutes=15))
        print("the first hourly fire started generating its (slow) report")

        await advance_until(clock, lambda: audit_skipped(1), step=timedelta(hours=1))
        audit_state = await audit.describe()
        print(
            "a later hourly fire was skipped while that report was still generating "
            f"(total_started={audit_state.total_started}, "
            f"total_skipped={audit_state.total_skipped})"
        )

        await advance_until(clock, lambda: audit_started(2), step=timedelta(hours=1), timeout=15)
        audit_state = await audit.describe()
        print(
            "once the slow report finished, the next fire started a new run "
            f"(total_started={audit_state.total_started}, "
            f"total_skipped={audit_state.total_skipped})"
        )

    print(rule("Every report generated"))
    for report in archive.generated:
        print(f"  {report.kind:8s} {report.region:6s} {report.report_id}")


if __name__ == "__main__":
    asyncio.run(main())
