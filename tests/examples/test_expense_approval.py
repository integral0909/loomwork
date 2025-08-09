"""Tests for ``examples.expense_approval``."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from examples.expense_approval import steps
from examples.expense_approval.models import Decision, ExpenseReport, Receipt
from examples.expense_approval.workflows import decision_channel, expense_report
from examples.support import advance_until, wait_for_query

if TYPE_CHECKING:
    from loomwork import ManualClock
    from tests.conftest import AppFactory

STEP_DEFS = [steps.request_approval, steps.send_reminder, steps.notify_outcome]
_LONG_STEP = timedelta(hours=25)


def _report(report_id: str) -> ExpenseReport:
    return ExpenseReport(
        report_id=report_id,
        employee="dana",
        manager="priya",
        director="sam",
        amount_cents=4200,
        description="conference travel",
    )


@pytest.fixture(autouse=True)
def _reset_mailbox() -> None:
    steps.mailbox.sent.clear()


async def test_manager_approves_after_a_receipt_is_attached(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([expense_report], STEP_DEFS)
    handle = await app.start_workflow(expense_report, _report("EXP-1"))

    await wait_for_query(clock, handle, "status", lambda s: bool(s))
    count = await handle.update("add_receipt", Receipt(filename="taxi.pdf", amount_cents=1500))
    assert count == 1

    await handle.signal(decision_channel, Decision(approved=True, by="priya", note="lgtm"))
    result = await handle.result(timeout=5)

    assert result.stage == "approved"
    assert result.approver == "priya"
    assert result.receipts == [Receipt(filename="taxi.pdf", amount_cents=1500)]
    assert steps.mailbox.count_to("priya") == 1
    assert ("dana", "Your expense report EXP-1 was approved") in steps.mailbox.sent


async def test_manager_rejects(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app([expense_report], STEP_DEFS)
    handle = await app.start_workflow(expense_report, _report("EXP-2"))
    await wait_for_query(clock, handle, "status", lambda s: bool(s))
    await handle.signal(decision_channel, Decision(approved=False, by="priya", note="not eligible"))
    result = await handle.result(timeout=5)
    assert result.stage == "rejected"
    assert result.approver == "priya"


async def test_escalates_to_director_after_two_missed_reminders(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([expense_report], STEP_DEFS)
    handle = await app.start_workflow(expense_report, _report("EXP-3"))
    await wait_for_query(clock, handle, "status", lambda s: bool(s))

    for _ in range(2):
        before = steps.mailbox.count_to("priya")

        def reminder_arrived(before: int = before) -> bool:
            return steps.mailbox.count_to("priya") > before

        await advance_until(clock, reminder_arrived, step=_LONG_STEP)

    # Wait for the director's notification itself, not just the "pending_director"
    # query result: the stage is updated in the same tick that *schedules* the step,
    # slightly before the step actually runs, so waiting on the query alone would
    # race with the step's completion under enough system load.
    await advance_until(clock, lambda: steps.mailbox.count_to("sam") >= 1, step=_LONG_STEP)
    stage = await handle.query("status")
    assert stage == {"stage": "pending_director", "receipt_count": 0}
    assert steps.mailbox.count_to("sam") == 1

    await handle.signal(decision_channel, Decision(approved=True, by="sam", note="director ok"))
    result = await handle.result(timeout=5)
    assert result.stage == "approved"
    assert result.approver == "sam"


async def test_expires_when_nobody_ever_responds(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app([expense_report], STEP_DEFS)
    handle = await app.start_workflow(expense_report, _report("EXP-4"))

    async def closed() -> bool:
        return (await handle.describe()).is_closed

    await advance_until(clock, closed, step=_LONG_STEP, timeout=15)
    result = await handle.result(timeout=5)

    assert result.stage == "expired"
    assert result.approver is None
    # Two reminders to the manager, then two to the director, then nobody.
    assert steps.mailbox.count_to("priya") == 3  # request + two reminders
    assert steps.mailbox.count_to("sam") == 3
