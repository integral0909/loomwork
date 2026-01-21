"""Steps for the support ticket workflow: paging on-call and notifying the customer.

Both are stand-ins for a real paging system (PagerDuty, Opsgenie, ...) and a real
notification channel (email, chat); the workflow only needs their journaled results.
"""

from __future__ import annotations

import loomwork as lw

#: Every escalation and notification triggered so far, in order -- a stand-in for
#: whatever real system these steps would actually call. Module-level state read by
#: ``main.py`` and tests, never by workflow code (see ``docs/lint-rules.md``'s
#: LW113 -- workflow code must never mutate a module global).
escalations: list[tuple[str, str]] = []
notifications: list[tuple[str, str]] = []


@lw.step(name="support_ticket.escalate_ticket")
async def escalate_ticket(ticket_id: str, _current_assignee: str) -> str:
    """Page the on-call lead for ``ticket_id`` and return the new assignee."""
    new_assignee = "oncall-lead"
    escalations.append((ticket_id, new_assignee))
    return new_assignee


@lw.step(name="support_ticket.notify_customer")
async def notify_customer(ticket_id: str, status: str) -> None:
    """Tell the customer ``ticket_id`` reached ``status``."""
    notifications.append((ticket_id, status))
