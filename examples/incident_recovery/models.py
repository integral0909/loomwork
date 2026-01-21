"""Pydantic models for the claims workflow."""

from __future__ import annotations

from pydantic import BaseModel


class Claim(BaseModel):
    claim_id: str
    claimant: str
    amount_cents: int
    flagged_fraud: bool = False


class ClaimResult(BaseModel):
    claim_id: str
    stage: str
    payout_cents: int | None = None
