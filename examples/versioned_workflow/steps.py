"""Steps shared by every version of the onboarding workflow."""

from __future__ import annotations

import loomwork as lw

#: Every notification actually sent, in order: (channel, user_id).
sent: list[tuple[str, str]] = []


@lw.step(name="versioned_workflow.send_email")
async def send_email(user_id: str) -> str:
    sent.append(("email", user_id))
    return f"email:{user_id}"


@lw.step(name="versioned_workflow.send_sms")
async def send_sms(user_id: str) -> str:
    sent.append(("sms", user_id))
    return f"sms:{user_id}"
