"""Steps for the scheduled reports example: building and publishing a report."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import loomwork as lw


@dataclass(slots=True)
class GeneratedReport:
    kind: str
    region: str
    report_id: str
    nominal_time: datetime | None


@dataclass(slots=True)
class ReportArchive:
    """Stands in for wherever generated reports actually get published."""

    generated: list[GeneratedReport] = field(default_factory=list)


archive = ReportArchive()


@lw.step(name="scheduled_reports.build_report")
async def build_report(kind: str, region: str, nominal_time: datetime | None) -> str:
    """Build (and "publish") one report, returning its id."""
    stamp = nominal_time.date().isoformat() if nominal_time is not None else "manual"
    report_id = f"{kind}-{region}-{stamp}"
    archive.generated.append(
        GeneratedReport(kind=kind, region=region, report_id=report_id, nominal_time=nominal_time)
    )
    return report_id
