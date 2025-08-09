"""Four versions of the same onboarding workflow, evolved while runs are in flight.

Every version is registered under the same workflow name, ``versioned_workflow.onboard``
-- only one is ever given to a running deployment at a time (see ``main.py``); all four
exist together here so the versioning tools (``loomwork.versioning``) can compare them
against recorded history.

**v1** always sends a welcome email, immediately (before any durable wait), then sleeps
for an hour before the workflow considers onboarding complete.

**v2** wants to switch to SMS, but must not change what a run already recorded under
v1 decides: it guards the new behaviour with :func:`loomwork.patched`, which replays
``False`` for a run whose history recorded this point before the patch existed, and
``True`` for a run reaching it for the first time (whether that run is brand new, or an
old run resuming after the deploy but which had not yet reached this decision).

**v2 (unsafe)** shows what happens *without* the guard: it looks identical to the
"after the patch" behaviour of v2, but breaks any history that already committed to the
email branch, because replaying it now emits a different command (schedule an SMS
step) than the one already recorded (schedule an email step).

**v3** is what v2 becomes once no run started before the patch existed is left:
:func:`loomwork.deprecate_patch` keeps histories that recorded the patch (v2's ``True``
branch) replaying correctly, without the ``if`` still needing to be there.
"""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw
from examples.versioned_workflow.steps import send_email, send_sms


@lw.workflow(name="versioned_workflow.onboard")
async def onboard_v1(user_id: str) -> str:
    sent = await send_email(user_id)
    await lw.sleep(timedelta(hours=1))  # a follow-up nudge, one hour after the welcome
    return sent


@lw.workflow(name="versioned_workflow.onboard")
async def onboard_v2(user_id: str) -> str:
    sent = await send_sms(user_id) if lw.patched("welcome-sms") else await send_email(user_id)
    await lw.sleep(timedelta(hours=1))
    return sent


@lw.workflow(name="versioned_workflow.onboard")
async def onboard_v2_unsafe(user_id: str) -> str:
    sent = await send_sms(user_id)  # no patch guard
    await lw.sleep(timedelta(hours=1))
    return sent


@lw.workflow(name="versioned_workflow.onboard")
async def onboard_v3(user_id: str) -> str:
    lw.deprecate_patch("welcome-sms")
    sent = await send_sms(user_id)
    await lw.sleep(timedelta(hours=1))
    return sent
