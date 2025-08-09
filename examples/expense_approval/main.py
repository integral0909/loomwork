"""Run the expense approval workflow through three scenarios.

.. code-block:: console

    uv run python -m examples.expense_approval.main

See ``README.md`` for what each scenario demonstrates.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import loomwork as lw
from examples.expense_approval.models import Decision, ExpenseReport, Receipt
from examples.expense_approval.steps import mailbox, notify_outcome, request_approval, send_reminder
from examples.expense_approval.workflows import decision_channel, expense_report
from examples.support import DEMO_POLL_INTERVAL, advance_until, rule, wait_for_query
from loomwork import ManualClock, MemoryStore
from loomwork.engine.config import EngineConfig
from loomwork.worker import WorkerOptions

_STEPS: list[lw.StepDefinition[..., Any]] = [request_approval, send_reminder, notify_outcome]
_LONG_STEP = timedelta(hours=25)  # comfortably past one 24h reminder interval per iteration


def _report(report_id: str, amount_cents: int) -> ExpenseReport:
    return ExpenseReport(
        report_id=report_id,
        employee="dana",
        manager="priya",
        director="sam",
        amount_cents=amount_cents,
        description="conference travel",
    )


async def main() -> None:
    clock = ManualClock()
    app = lw.Loomwork(
        store=MemoryStore(),
        workflows=[expense_report],
        steps=_STEPS,
        clock=clock,
        config=EngineConfig(poll_interval=DEMO_POLL_INTERVAL),
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with app:
        print(rule("Scenario 1: manager approves promptly, after a receipt is attached"))
        quick = await app.start_workflow(expense_report, _report("EXP-1", 42_00))
        await wait_for_query(clock, quick, "status", lambda s: bool(s))
        count = await quick.update("add_receipt", Receipt(filename="taxi.pdf", amount_cents=1500))
        print(f"receipt attached; report now has {count} receipt(s)")
        await quick.signal(decision_channel, Decision(approved=True, by="priya", note="approved"))
        quick_result = await quick.result(timeout=5)
        print(f"result: {quick_result.model_dump()}")

        print(rule("Scenario 2: manager misses two reminders, so it escalates to the director"))
        escalated = await app.start_workflow(expense_report, _report("EXP-2", 980_00))
        await wait_for_query(clock, escalated, "status", lambda s: bool(s))
        for reminder_number in (1, 2):
            before = mailbox.count_to("priya")

            def another_reminder_arrived(before: int = before) -> bool:
                return mailbox.count_to("priya") > before

            await advance_until(clock, another_reminder_arrived, step=_LONG_STEP)
            print(f"reminder #{reminder_number} sent to the manager")
        stage = await wait_for_query(
            clock, escalated, "status", lambda s: s["stage"] == "pending_director", step=_LONG_STEP
        )
        print(f"after 3 days of silence from the manager: {stage}")
        await escalated.signal(
            decision_channel, Decision(approved=True, by="sam", note="director sign-off")
        )
        escalated_result = await escalated.result(timeout=5)
        print(f"result: {escalated_result.model_dump()}")

        print(rule("Scenario 3: nobody ever responds, so the report expires"))
        ignored = await app.start_workflow(expense_report, _report("EXP-3", 250_00))

        async def _closed() -> bool:
            return (await ignored.describe()).is_closed

        await advance_until(clock, _closed, step=_LONG_STEP, timeout=15)
        ignored_result = await ignored.result(timeout=5)
        print(f"result: {ignored_result.model_dump()}")

    print(rule("Notifications sent"))
    for to, subject in mailbox.sent:
        print(f"  to {to}: {subject}")


if __name__ == "__main__":
    asyncio.run(main())
