"""The report workflow driven by every schedule in this example.

One workflow definition serves daily, weekly and hourly schedules alike; what varies is
the arguments a schedule passes it (see ``main.py``), and, for the deliberately slow
hourly audit schedule, how long report generation takes.
"""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw
from examples.scheduled_reports.models import ReportResult
from examples.scheduled_reports.steps import build_report


@lw.workflow(name="scheduled_reports.generate_report")
async def generate_report(kind: str, region: str, duration_minutes: float = 0.0) -> ReportResult:
    """Generate one report for ``region``.

    ``loomwork.info().scheduled_time`` is the schedule's *nominal* fire time -- the
    date the report is about -- which stays the same even if the run itself starts
    late (after an outage) or, as with the hourly audit schedule below, is delayed
    behind a still-running previous run.
    """
    info = lw.info()
    if duration_minutes:
        await lw.sleep(timedelta(minutes=duration_minutes))
    report_id = await build_report(kind, region, info.scheduled_time)
    return ReportResult(
        kind=kind,
        region=region,
        report_id=report_id,
        nominal_time=info.scheduled_time,
        schedule_id=info.schedule_id,
    )
