"""Pydantic models for the scheduled reports example."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ReportResult(BaseModel):
    kind: str
    region: str
    report_id: str
    nominal_time: datetime | None
    schedule_id: str | None
