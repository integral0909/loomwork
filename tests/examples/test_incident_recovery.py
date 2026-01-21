"""Tests for ``examples.incident_recovery``."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

import loomwork as lw
from examples.incident_recovery import steps
from examples.incident_recovery.models import Claim, ClaimResult
from examples.incident_recovery.workflows import DEFAULT_REVIEW_WINDOW, process_claim
from examples.support import wait_for_query
from loomwork import IdConflictPolicy, ResetPoint

if TYPE_CHECKING:
    from loomwork import ManualClock
    from tests.conftest import AppFactory

BUGGY_STEPS = [steps.validate_claim, steps.compute_payout]
FIXED_STEPS = [steps.validate_claim, steps.compute_payout_fixed]

QUERY = "workflow_name = 'incident_recovery.process_claim' AND status = 'running'"


@pytest.fixture(autouse=True)
def _reset_downstream_state() -> None:
    steps.validated.clear()
    steps.paid.clear()


async def test_resetting_the_run_recovers_it_once_the_bug_is_fixed(
    make_app: AppFactory, clock: ManualClock
) -> None:
    buggy = await make_app([process_claim], BUGGY_STEPS)
    claim = Claim(claim_id="claim-VIP", claimant="Acme Corp", amount_cents=250_000_00)
    handle = await buggy.start_workflow(process_claim, claim, timedelta(minutes=10), id="claim-VIP")
    await wait_for_query(
        clock, handle, "status", lambda s: s == "awaiting_review", step=timedelta(0)
    )
    await clock.advance(timedelta(minutes=11))
    with pytest.raises(lw.WorkflowFailedError) as excinfo:
        await handle.result(timeout=5)
    assert excinfo.value.failure.type == "StepError"
    assert excinfo.value.failure.cause is not None
    assert excinfo.value.failure.cause.type == "PayoutRejected"
    await buggy.stop()

    fixed = await make_app([process_claim], FIXED_STEPS, store=buggy.store)
    failed_handle = fixed.get_handle(workflow_id="claim-VIP", result_type=ClaimResult)
    recovered = await failed_handle.reset(
        to=ResetPoint.before_last_failure(), reason="fixed compute_payout"
    )
    result = await recovered.result(timeout=5)

    assert result.stage == "paid"
    assert result.payout_cents == 250_000_00
    assert steps.paid == [("claim-VIP", 250_000_00)]
    # Validation was not repeated: the reset kept everything before the failing tick.
    assert steps.validated == ["claim-VIP"]


async def test_batch_terminate_pauses_every_run_still_in_flight(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([process_claim], BUGGY_STEPS)
    claim_a = await app.start_workflow(
        process_claim,
        Claim(claim_id="claim-A", claimant="A. Homeowner", amount_cents=50_000_00),
        DEFAULT_REVIEW_WINDOW,
        id="claim-A",
    )
    claim_b = await app.start_workflow(
        process_claim,
        Claim(claim_id="claim-B", claimant="B. Driver", amount_cents=75_000_00),
        DEFAULT_REVIEW_WINDOW,
        id="claim-B",
    )
    for handle in (claim_a, claim_b):
        await wait_for_query(
            clock, handle, "status", lambda s: s == "awaiting_review", step=timedelta(0)
        )

    summary = await app.client.batch_terminate(QUERY, reason="incident: pausing in-flight claims")

    assert summary.matched == 2
    assert summary.succeeded == 2
    assert summary.failed == ()
    for handle in (claim_a, claim_b):
        with pytest.raises(lw.WorkflowTerminatedError) as excinfo:
            await handle.result(timeout=5)
        assert excinfo.value.reason == "incident: pausing in-flight claims"
    # Nothing reached the (never-fixed, in this test) buggy payout step.
    assert steps.paid == []


async def test_use_existing_makes_a_duplicate_submission_a_no_op(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([process_claim], BUGGY_STEPS)
    claim = Claim(claim_id="claim-A", claimant="A. Homeowner", amount_cents=50_000_00)
    first = await app.start_workflow(process_claim, claim, DEFAULT_REVIEW_WINDOW, id="claim-A")

    retried = await app.start_workflow(
        process_claim,
        claim,
        DEFAULT_REVIEW_WINDOW,
        id="claim-A",
        id_conflict_policy=IdConflictPolicy.USE_EXISTING,
    )

    assert retried.run_id == first.run_id
    await wait_for_query(
        clock, first, "status", lambda s: s == "awaiting_review", step=timedelta(0)
    )
    assert steps.validated == ["claim-A"]  # validated exactly once, not twice
