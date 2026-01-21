"""Insurance claim processing: validate, wait out a review window, compute payout.

The workflow does not catch the payout step's failure -- a bug in that step (see
``steps.py``) therefore fails the *run* itself, cleanly and deterministically, exactly
the scenario ``docs/operations/recovery.md``'s reset walkthrough is about.
"""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw
from examples.incident_recovery.models import Claim, ClaimResult
from examples.incident_recovery.steps import compute_payout, validate_claim

#: How long a claim sits in review before its payout is computed. A workflow argument
#: (rather than a hard-coded constant), like ``order_fulfillment``'s cancellation
#: window, so a demo can shrink it for one claim without changing what the durable wait
#: itself does.
DEFAULT_REVIEW_WINDOW = timedelta(hours=6)


@lw.workflow(name="incident_recovery.process_claim")
async def process_claim(
    claim: Claim, review_window: timedelta = DEFAULT_REVIEW_WINDOW
) -> ClaimResult:
    stage = "validating"

    @lw.query("status")
    def status() -> str:
        return stage

    await validate_claim(claim.claim_id)
    stage = "awaiting_review"
    await lw.sleep(review_window)
    stage = "computing_payout"
    payout_cents = await compute_payout(claim.claim_id, claim.amount_cents, claim.flagged_fraud)
    stage = "paid"
    return ClaimResult(claim_id=claim.claim_id, stage=stage, payout_cents=payout_cents)
