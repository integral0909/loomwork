"""Run the support ticket workflow through four scenarios.

.. code-block:: console

    uv run python -m examples.support_ticket.main

See ``README.md`` for what each scenario demonstrates.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import loomwork as lw
from examples.support import DEMO_POLL_INTERVAL, rule, wait_for_query
from examples.support_ticket import steps
from examples.support_ticket.workflows import FIRST_RESPONSE_SLA, RESOLUTION_SLA, Ticket
from loomwork import ManualClock, MemoryStore
from loomwork.engine.config import EngineConfig
from loomwork.worker import WorkerOptions

_STEPS: list[lw.StepDefinition[..., Any]] = [steps.escalate_ticket, steps.notify_customer]


async def main() -> None:
    clock = ManualClock()
    app = lw.Loomwork(
        store=MemoryStore(),
        workflows=[Ticket],
        steps=_STEPS,
        clock=clock,
        config=EngineConfig(poll_interval=DEMO_POLL_INTERVAL),
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with app:
        print(rule("Scenario 1: answered and closed well within the first-response SLA"))
        quick = await app.start_workflow(Ticket, "T-1", "printer jam", "agent-1", id="ticket-T-1")
        await quick.signal(Ticket.reply, "on it -- please power-cycle the printer")
        await quick.signal(Ticket.close_ticket, "resolved by a restart")
        result = await quick.result(timeout=5)
        print(f"T-1 result: {result!r}; summary: {await quick.query(Ticket.summary)}")

        print(rule("Scenario 2: no response in time -- escalates, then gets closed"))
        escalated = await app.start_workflow(
            Ticket, "T-2", "billing dispute", "agent-2", id="ticket-T-2"
        )
        # Let the first tick register the ticket's queries before jumping the clock.
        await wait_for_query(clock, escalated, "summary", lambda _: True, step=timedelta(0))
        await clock.advance(FIRST_RESPONSE_SLA + timedelta(minutes=1))
        summary = await wait_for_query(
            clock, escalated, "summary", lambda s: bool(s["escalated"]), step=timedelta(minutes=1)
        )
        print(f"T-2 escalated to {summary['assignee']!r} after missing the first-response SLA")
        await escalated.signal(Ticket.reply, "refund issued")
        await escalated.signal(Ticket.close_ticket, "refund issued")
        result = await escalated.result(timeout=5)
        print(f"T-2 result: {result!r}")

        print(rule("Scenario 3: nobody ever responds -- the ticket expires on its own"))
        expired = await app.start_workflow(
            Ticket, "T-3", "feature request", "agent-3", id="ticket-T-3"
        )
        await wait_for_query(clock, expired, "summary", lambda _: True, step=timedelta(0))
        await clock.advance(FIRST_RESPONSE_SLA + timedelta(minutes=1))
        # The clock advances in two hops, matching the workflow's two chained SLA
        # windows: the second window's timeout is relative to when escalation actually
        # happens, so it must be let to happen before advancing past it too.
        await wait_for_query(
            clock, expired, "summary", lambda s: bool(s["escalated"]), step=timedelta(minutes=1)
        )
        await clock.advance(RESOLUTION_SLA - FIRST_RESPONSE_SLA + timedelta(minutes=1))
        result = await expired.result(timeout=5)
        print(f"T-3 result: {result!r}")

        print(rule("Scenario 4: reassigning a ticket, with validation"))
        live = await app.start_workflow(Ticket, "T-4", "outage", "agent-4", id="ticket-T-4")
        previous = await live.update(Ticket.reassign, "agent-5")
        print(f"T-4 reassigned from {previous!r} to 'agent-5'")
        for bad_assignee, label in [("", "a blank assignee"), ("agent-5", "a no-op reassignment")]:
            try:
                await live.update(Ticket.reassign, bad_assignee)
            except lw.UpdateFailedError as exc:
                print(f"rejected {label}: {exc.failure.type}: {exc.failure.message}")
        await live.signal(Ticket.close_ticket, "outage resolved")
        await live.result(timeout=5)

    print(rule("Everything that happened downstream"))
    print(f"escalations: {steps.escalations}")
    print(f"notifications: {steps.notifications}")


if __name__ == "__main__":
    asyncio.run(main())
