"""Steps for the expense approval workflow: outbound notifications.

A real deployment would call an email or chat API here; this stand-in just records
what was sent so the demo and its tests can observe it.
"""

from __future__ import annotations

import loomwork as lw


class Mailbox:
    """Records every notification sent, in order."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []  # (to, subject)

    def deliver(self, to: str, subject: str) -> None:
        self.sent.append((to, subject))

    def count_to(self, to: str) -> int:
        return sum(1 for recipient, _ in self.sent if recipient == to)


#: Stands in for the mail/chat system a real deployment would call from these steps.
mailbox = Mailbox()


@lw.step(name="expense_approval.request_approval")
async def request_approval(report_id: str, approver: str) -> None:
    """Ask ``approver`` to review report ``report_id`` for the first time."""
    mailbox.deliver(approver, f"Please review expense report {report_id}")


@lw.step(name="expense_approval.send_reminder")
async def send_reminder(report_id: str, approver: str) -> None:
    """Nudge ``approver`` that report ``report_id`` is still awaiting their decision."""
    mailbox.deliver(approver, f"Reminder: expense report {report_id} is still awaiting review")


@lw.step(name="expense_approval.notify_outcome")
async def notify_outcome(report_id: str, employee: str, stage: str) -> None:
    """Tell the employee how report ``report_id`` was resolved."""
    mailbox.deliver(employee, f"Your expense report {report_id} was {stage}")
