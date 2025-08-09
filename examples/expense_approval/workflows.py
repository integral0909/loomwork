"""Human-in-the-loop expense approval.

A manager has up to three days to approve or reject a report, nudged by a reminder
every 24 hours; if the window lapses without a decision, the report escalates to a
director, who gets the same three-day, reminder-backed window. The employee can attach
receipts at any point before a decision is recorded.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import loomwork as lw
from examples.expense_approval.models import Decision, ExpenseReport, ExpenseResult, Receipt
from examples.expense_approval.steps import notify_outcome, request_approval, send_reminder

#: The approver's decision, sent back through this channel.
decision_channel: lw.Channel[Decision] = lw.Channel("expense_approval.decision", Decision)

APPROVAL_WINDOW = timedelta(days=3)
REMINDER_INTERVAL = timedelta(hours=24)


async def _await_decision(report_id: str, approver: str) -> Decision | None:
    """Wait up to :data:`APPROVAL_WINDOW` for ``approver``'s decision.

    Sends :func:`~examples.expense_approval.steps.send_reminder` every
    :data:`REMINDER_INTERVAL` while waiting, and gives up (returning ``None``) once the
    whole window has elapsed without a decision. A plain module-level helper rather
    than a step or a second workflow: it only sleeps, waits on a channel and calls
    steps, all of which replay safely, so it needs none of the durability machinery a
    step or child workflow would add.
    """
    remaining = APPROVAL_WINDOW
    while remaining > timedelta(0):
        wait = min(remaining, REMINDER_INTERVAL)
        try:
            async with asyncio.timeout(wait.total_seconds()):
                return await decision_channel.receive()
        except TimeoutError:
            remaining -= wait
            if remaining > timedelta(0):
                await send_reminder(report_id, approver)
    return None


@lw.workflow(name="expense_approval.expense_report")
async def expense_report(report: ExpenseReport) -> ExpenseResult:
    receipts: list[Receipt] = []
    stage = "pending_manager"

    @lw.query("status")
    def status() -> dict[str, object]:
        return {"stage": stage, "receipt_count": len(receipts)}

    @lw.update("add_receipt")
    async def add_receipt(receipt: Receipt) -> int:
        receipts.append(receipt)
        return len(receipts)

    await request_approval(report.report_id, report.manager)
    decision = await _await_decision(report.report_id, report.manager)

    if decision is None:
        stage = "pending_director"
        await request_approval(report.report_id, report.director)
        decision = await _await_decision(report.report_id, report.director)
        if decision is None:
            stage = "expired"
            await notify_outcome(report.report_id, report.employee, stage)
            return ExpenseResult(report_id=report.report_id, stage=stage, receipts=list(receipts))

    stage = "approved" if decision.approved else "rejected"
    await notify_outcome(report.report_id, report.employee, stage)
    return ExpenseResult(
        report_id=report.report_id,
        stage=stage,
        approver=decision.by,
        note=decision.note,
        receipts=list(receipts),
    )
