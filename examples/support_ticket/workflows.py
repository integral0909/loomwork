"""A support ticket with SLA timers, escalation and customer replies.

Modeled as a class (see ``docs/guides/workflow-classes.md``): state -- status, assignee,
the reply thread -- lives in attributes, and every kind of message the outside world can
send (a customer reply, a request to close, a reassignment) is a method instead of a
branch inside one long function.

``run`` waits for the ticket to close, using ``lw.wait_condition`` with a durable
timeout for each of two SLA stages. If nobody replies within ``FIRST_RESPONSE_SLA``, the
ticket escalates to the on-call lead and gets a second, longer window; if that one also
lapses, the ticket closes itself as expired. A reply alone does not close a ticket --
only an explicit ``close`` signal (an agent's decision) does -- so the SLA timers keep
counting down through a reply that does not resolve the issue.
"""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw
from examples.support_ticket.steps import escalate_ticket, notify_customer

#: How long a ticket may go unanswered before it escalates to the on-call lead.
FIRST_RESPONSE_SLA = timedelta(hours=4)
#: Total time (from creation) a ticket may stay open before it is closed as expired.
RESOLUTION_SLA = timedelta(days=3)


@lw.workflow(name="support_ticket.ticket")
class Ticket:
    def __init__(self, ticket_id: str, subject: str, assignee: str) -> None:
        self.ticket_id = ticket_id
        self.subject = subject
        self.assignee = assignee
        self.status = "open"
        self.escalated = False
        self.replies: list[str] = []

    @lw.run
    async def run(self, _ticket_id: str, _subject: str, _assignee: str) -> str:
        try:
            await lw.wait_condition(lambda: self.status == "closed", timeout=FIRST_RESPONSE_SLA)
        except TimeoutError:
            self.escalated = True
            self.assignee = await escalate_ticket(self.ticket_id, self.assignee)
            try:
                await lw.wait_condition(
                    lambda: self.status == "closed",
                    timeout=RESOLUTION_SLA - FIRST_RESPONSE_SLA,
                )
            except TimeoutError:
                self.status = "expired"
        await notify_customer(self.ticket_id, self.status)
        return self.status

    @lw.signal
    def reply(self, text: str) -> None:
        """A customer (or agent) reply. Does not by itself close the ticket."""
        self.replies.append(text)

    @lw.signal("close")
    def close_ticket(self, reason: str) -> None:
        self.status = "closed"
        self.replies.append(f"[closed: {reason}]" if reason else "[closed]")

    @lw.query
    def summary(self) -> dict[str, object]:
        return {
            "status": self.status,
            "assignee": self.assignee,
            "reply_count": len(self.replies),
            "escalated": self.escalated,
        }

    @lw.update
    def reassign(self, new_assignee: str) -> str:
        """Hand the ticket to ``new_assignee``. Returns the previous assignee.

        Raises an :class:`~loomwork.ApplicationError` -- returned to the caller of the
        update, without failing the run (see ``docs/guides/workflow-classes.md``'s
        handler semantics) -- for a blank assignee or a no-op reassignment.
        """
        if not new_assignee.strip():
            raise lw.ApplicationError("assignee must not be blank", type="InvalidAssignee")
        if new_assignee == self.assignee:
            raise lw.ApplicationError(
                f"ticket is already assigned to {new_assignee!r}", type="NoOpReassign"
            )
        previous = self.assignee
        self.assignee = new_assignee
        return previous
