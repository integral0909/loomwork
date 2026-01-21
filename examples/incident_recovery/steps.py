"""Steps for the claims workflow, including the buggy (and later fixed) payout
calculation this example's incident is about.
"""

from __future__ import annotations

import loomwork as lw

#: Every claim that got past validation, and every payout actually computed, in order --
#: a stand-in for a real claims database.
validated: list[str] = []
paid: list[tuple[str, int]] = []


@lw.step(name="incident_recovery.validate_claim")
async def validate_claim(claim_id: str) -> None:
    validated.append(claim_id)


@lw.step(name="incident_recovery.compute_payout")
async def compute_payout(claim_id: str, amount_cents: int, flagged_fraud: bool) -> int:
    """The buggy version: rejects any claim over $1,000 -- not just fraud-flagged ones.

    A coding mistake (an extra, wrong condition), not a business rule: real, legitimate
    high-value claims fail because of it. That is the incident this example recovers
    from; see :func:`compute_payout_fixed` for the corrected step.
    """
    if flagged_fraud or amount_cents > 100_000:
        raise lw.ApplicationError("payout rejected", type="PayoutRejected", non_retryable=True)
    paid.append((claim_id, amount_cents))
    return amount_cents


@lw.step(name="incident_recovery.compute_payout")
async def compute_payout_fixed(claim_id: str, amount_cents: int, flagged_fraud: bool) -> int:
    """The fix: only fraud-flagged claims are rejected, as was always intended."""
    if flagged_fraud:
        raise lw.ApplicationError("payout rejected", type="PayoutRejected", non_retryable=True)
    paid.append((claim_id, amount_cents))
    return amount_cents
