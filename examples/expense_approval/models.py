"""Pydantic models for the expense approval workflow."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ExpenseReport(BaseModel):
    report_id: str
    employee: str
    manager: str
    director: str
    amount_cents: int = Field(gt=0)
    description: str


class Receipt(BaseModel):
    filename: str
    amount_cents: int = Field(ge=0)


class Decision(BaseModel):
    approved: bool
    by: str
    note: str = ""


class ExpenseResult(BaseModel):
    report_id: str
    stage: str
    approver: str | None = None
    note: str = ""
    receipts: list[Receipt] = Field(default_factory=list)
