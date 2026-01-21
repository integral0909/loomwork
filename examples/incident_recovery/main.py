"""Operator recovery from a step bug: fix the code, reset one high-priority run, and
batch-terminate the rest -- plus an idempotent restart during the incident.

.. code-block:: console

    uv run python -m examples.incident_recovery.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import loomwork as lw
from examples.incident_recovery import steps
from examples.incident_recovery.models import Claim, ClaimResult
from examples.incident_recovery.workflows import DEFAULT_REVIEW_WINDOW, process_claim
from examples.support import DEMO_POLL_INTERVAL, rule, wait_for_query
from loomwork import IdConflictPolicy, ManualClock, MemoryStore, ResetPoint
from loomwork.engine.config import EngineConfig
from loomwork.worker import WorkerOptions

QUERY = "workflow_name = 'incident_recovery.process_claim' AND status = 'running'"


async def main() -> None:
    clock = ManualClock()
    store = MemoryStore()
    config = EngineConfig(poll_interval=DEMO_POLL_INTERVAL)
    worker_options = WorkerOptions(shutdown_grace=0.2)

    print(rule("Three claims come in; one is a big, legitimate, time-sensitive claim"))
    buggy = lw.Loomwork(
        store=store,
        workflows=[process_claim],
        steps=[steps.validate_claim, steps.compute_payout],
        clock=clock,
        config=config,
        worker_options=worker_options,
    )
    async with buggy:
        vip = await buggy.start_workflow(
            process_claim,
            Claim(claim_id="claim-VIP", claimant="Acme Corp", amount_cents=250_000_00),
            timedelta(minutes=10),  # a short review window: this claim is urgent
            id="claim-VIP",
        )
        claim_a = await buggy.start_workflow(
            process_claim,
            Claim(claim_id="claim-A", claimant="A. Homeowner", amount_cents=50_000_00),
            DEFAULT_REVIEW_WINDOW,
            id="claim-A",
        )
        claim_b = await buggy.start_workflow(
            process_claim,
            Claim(claim_id="claim-B", claimant="B. Driver", amount_cents=75_000_00),
            DEFAULT_REVIEW_WINDOW,
            id="claim-B",
        )

        print(rule("A webhook retries claim-A's submission; USE_EXISTING makes it a no-op"))
        retried = await buggy.start_workflow(
            process_claim,
            Claim(claim_id="claim-A", claimant="A. Homeowner", amount_cents=50_000_00),
            DEFAULT_REVIEW_WINDOW,
            id="claim-A",
            id_conflict_policy=IdConflictPolicy.USE_EXISTING,
        )
        assert retried.run_id == claim_a.run_id
        print(f"resubmission returned the same run: {retried.run_id}")

        print(rule("claim-VIP's short review window elapses; the payout bug rejects it"))
        await wait_for_query(
            clock, vip, "status", lambda s: s == "awaiting_review", step=timedelta(0)
        )
        await clock.advance(timedelta(minutes=11))
        try:
            await vip.result(timeout=5)
        except lw.WorkflowFailedError as exc:
            print(f"claim-VIP failed: {exc.failure.type}: {exc.failure.message}")
        # claim-A and claim-B are unaffected: their review window (six hours) has not
        # elapsed, so they are still waiting -- and still running -- when the incident
        # is declared below.

    print(rule("The bug is found and fixed: compute_payout's body is corrected"))
    fixed = lw.Loomwork(
        store=store,
        workflows=[process_claim],  # unchanged: only the step's implementation moved
        steps=[steps.validate_claim, steps.compute_payout_fixed],
        clock=clock,
        config=config,
        worker_options=worker_options,
    )
    async with fixed:
        print(rule("claim-VIP is worth recovering individually: reset to before the failure"))
        failed_vip = fixed.get_handle(workflow_id="claim-VIP", result_type=ClaimResult)
        recovered_vip = await failed_vip.reset(
            to=ResetPoint.before_last_failure(),
            reason="fixed compute_payout: only fraud-flagged claims are rejected",
        )
        vip_result = await recovered_vip.result(timeout=5)
        print(f"claim-VIP recovered: {vip_result.model_dump()}")

        print(rule("Everything else still in flight is paused for the incident"))
        summary = await fixed.client.batch_terminate(
            QUERY, reason="incident: pausing in-flight claims while the payout fix is verified"
        )
        print(f"batch_terminate: {summary}")

        for handle, claim_id in ((claim_a, "claim-A"), (claim_b, "claim-B")):
            try:
                await handle.result(timeout=5)
            except lw.WorkflowTerminatedError as exc:
                print(f"{claim_id}: terminated ({exc.reason})")

    print(rule("Downstream state"))
    print(f"validated: {steps.validated}")
    print(f"paid: {steps.paid}")


if __name__ == "__main__":
    asyncio.run(main())
