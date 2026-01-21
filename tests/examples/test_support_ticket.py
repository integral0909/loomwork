"""Tests for ``examples.support_ticket``."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

import loomwork as lw
from examples.support import wait_for_query
from examples.support_ticket.steps import escalate_ticket, notify_customer
from examples.support_ticket.workflows import FIRST_RESPONSE_SLA, RESOLUTION_SLA, Ticket
from loomwork.testing import WorkflowEnvironment

STEP_DEFS: list[lw.StepDefinition[..., Any]] = [escalate_ticket, notify_customer]


async def test_closed_within_the_first_response_sla_never_escalates() -> None:
    async with WorkflowEnvironment(workflows=[Ticket], steps=STEP_DEFS) as env:
        escalate_mock = env.mock_step(escalate_ticket)
        notify_mock = env.mock_step(notify_customer)
        handle = await env.start_workflow(Ticket, "T-1", "printer jam", "agent-1", id="ticket-T-1")
        await handle.signal(Ticket.reply, "please restart the printer")
        await handle.signal(Ticket.close_ticket, "resolved by a restart")
        result = await handle.result(timeout=5)

    assert result == "closed"
    escalate_mock.assert_not_called()
    notify_mock.assert_called_once()
    notify_mock.assert_called_with("T-1", "closed")
    assert (await handle.query(Ticket.summary))["reply_count"] == 2  # reply + close note


async def test_missing_the_sla_escalates_then_can_still_be_closed() -> None:
    async with WorkflowEnvironment(
        workflows=[Ticket], steps=STEP_DEFS, auto_time_skipping=False
    ) as env:
        escalate_mock = env.mock_step(escalate_ticket, return_value="oncall-lead")
        notify_mock = env.mock_step(notify_customer)
        handle = await env.start_workflow(
            Ticket, "T-2", "billing dispute", "agent-2", id="ticket-T-2"
        )
        # Let the first tick register the ticket's queries before jumping the clock.
        await wait_for_query(env.clock, handle, "summary", lambda _: True, step=timedelta(0))
        await env.sleep(FIRST_RESPONSE_SLA + timedelta(minutes=1))
        summary = await wait_for_query(
            env.clock, handle, "summary", lambda s: bool(s["escalated"]), step=timedelta(minutes=1)
        )
        assert summary["assignee"] == "oncall-lead"
        escalate_mock.assert_called_once()
        escalate_mock.assert_called_with("T-2", "agent-2")

        await handle.signal(Ticket.reply, "refund issued")
        await handle.signal(Ticket.close_ticket, "refund issued")
        result = await handle.result(timeout=5)

    assert result == "closed"
    notify_mock.assert_called_once()
    notify_mock.assert_called_with("T-2", "closed")


async def test_no_response_at_all_expires_the_ticket() -> None:
    async with WorkflowEnvironment(
        workflows=[Ticket], steps=STEP_DEFS, auto_time_skipping=False
    ) as env:
        escalate_mock = env.mock_step(escalate_ticket, return_value="oncall-lead")
        notify_mock = env.mock_step(notify_customer)
        handle = await env.start_workflow(
            Ticket, "T-3", "feature request", "agent-3", id="ticket-T-3"
        )
        await wait_for_query(env.clock, handle, "summary", lambda _: True, step=timedelta(0))
        await env.sleep(FIRST_RESPONSE_SLA + timedelta(minutes=1))
        await wait_for_query(
            env.clock, handle, "summary", lambda s: bool(s["escalated"]), step=timedelta(minutes=1)
        )
        escalate_mock.assert_called_once()
        escalate_mock.assert_called_with("T-3", "agent-3")
        await env.sleep(RESOLUTION_SLA - FIRST_RESPONSE_SLA + timedelta(minutes=1))
        result = await handle.result(timeout=5)

    assert result == "expired"
    notify_mock.assert_called_once()
    notify_mock.assert_called_with("T-3", "expired")


async def test_reassign_validates_the_new_assignee() -> None:
    async with WorkflowEnvironment(
        workflows=[Ticket], steps=STEP_DEFS, auto_time_skipping=False
    ) as env:
        env.mock_step(notify_customer)
        handle = await env.start_workflow(Ticket, "T-4", "outage", "agent-4", id="ticket-T-4")

        previous = await handle.update(Ticket.reassign, "agent-5")
        assert previous == "agent-4"
        assert (await handle.query(Ticket.summary))["assignee"] == "agent-5"

        with pytest.raises(lw.UpdateFailedError) as blank:
            await handle.update(Ticket.reassign, "   ")
        assert blank.value.failure.type == "InvalidAssignee"

        with pytest.raises(lw.UpdateFailedError) as no_op:
            await handle.update(Ticket.reassign, "agent-5")
        assert no_op.value.failure.type == "NoOpReassign"

        await handle.signal(Ticket.close_ticket, "outage resolved")
        result = await handle.result(timeout=5)

    assert result == "closed"
