"""The workflow from ``docs/tutorial.md``, run for real.

The code below is the tutorial's, with the payment gateway and mailer replaced by
recording stubs, so the tutorial cannot silently drift from what Loomwork does.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

import loomwork as lw
from loomwork.testing import WorkflowEnvironment

pytestmark = pytest.mark.integration

sent: list[tuple[str, str]] = []
charges: list[str] = []
declined: set[str] = set()


@lw.step(
    name="billing.charge",
    retry=lw.RetryPolicy(initial_interval=30, backoff=2.0, max_attempts=4),
    timeout=timedelta(seconds=20),
)
async def charge(customer_id: str, cents: int, idempotency_key: str) -> str:
    if idempotency_key in declined:
        raise lw.ApplicationError("insufficient funds", type="CardDeclined", non_retryable=True)
    charges.append(idempotency_key)
    return f"ch_{len(charges)}"


@lw.step(name="billing.email")
async def email(customer_id: str, template: str) -> None:
    sent.append((customer_id, template))


@lw.workflow(name="billing.subscription")
class Subscription:
    def __init__(self, customer_id: str, cents: int) -> None:
        self.paused = False
        self.has_new_card = False
        self.renewals = 0

    @lw.run
    async def run(self, customer_id: str, cents: int) -> str:
        while True:
            await lw.sleep(timedelta(days=30))
            await lw.wait_condition(lambda: not self.paused)

            key = f"{lw.info().workflow_id}/{self.renewals}"
            try:
                await charge(customer_id, cents, key)
            except lw.StepError:
                if not await self.grace_period(customer_id):
                    await email(customer_id, "subscription-cancelled")
                    return "cancelled"
                await charge(customer_id, cents, key + "/retry")
            self.renewals += 1
            await email(customer_id, "receipt")
            if lw.continue_as_new_suggested():
                lw.continue_as_new(customer_id, cents)

    async def grace_period(self, customer_id: str) -> bool:
        await email(customer_id, "payment-failed")
        self.has_new_card = False
        try:
            await lw.wait_condition(lambda: self.has_new_card, timeout=timedelta(days=7))
        except TimeoutError:
            return False
        return True

    @lw.signal
    def card_updated(self, _: bool) -> None:
        self.has_new_card = True

    @lw.signal
    def set_paused(self, paused: bool) -> None:
        self.paused = paused

    @lw.query
    def status(self) -> dict[str, object]:
        return {"paused": self.paused, "renewals": self.renewals}


@pytest.fixture(autouse=True)
def _reset() -> None:
    sent.clear()
    charges.clear()
    declined.clear()


def environment(**kwargs: Any) -> WorkflowEnvironment:
    return WorkflowEnvironment(workflows=[Subscription], steps=[charge, email], **kwargs)


async def test_declined_card_without_update_cancels() -> None:
    # The test from the tutorial, using mocks.
    async with environment() as env:
        env.mock_step(
            charge,
            side_effect=lw.ApplicationError("declined", type="CardDeclined", non_retryable=True),
        )
        emails = env.mock_step(email, return_value=None)

        result = await env.execute(Subscription, "cus_1", 999)

        assert result == "cancelled"
        templates = [call.args[1] for call in emails.calls]
        assert templates == ["payment-failed", "subscription-cancelled"]


async def test_monthly_renewals_charge_once_per_month() -> None:
    async with environment(auto_time_skipping=False) as env:
        handle = await env.start_workflow(Subscription, "cus_2", 999, id="sub-2")
        for month in range(1, 4):
            await _until_timer(env)  # the next renewal is scheduled
            await env.sleep(timedelta(days=30))
            await _until(lambda month=month: len(charges) == month)
        assert charges == ["sub-2/0", "sub-2/1", "sub-2/2"]
        assert await handle.query(Subscription.status) == {"paused": False, "renewals": 3}


async def test_updating_the_card_during_the_grace_period_recovers() -> None:
    declined.add("sub-3/0")
    async with environment(auto_time_skipping=False) as env:
        handle = await env.start_workflow(Subscription, "cus_3", 999, id="sub-3")
        await _until_registered(handle)
        await env.sleep(timedelta(days=30))
        await _until(lambda: ("cus_3", "payment-failed") in sent)
        await env.sleep(timedelta(days=2))
        await handle.signal(Subscription.card_updated, True)
        await _until(lambda: charges == ["sub-3/0/retry"])
        await _until(lambda: ("cus_3", "receipt") in sent)
        assert not (await handle.describe()).is_closed


async def test_paused_subscriptions_skip_charging_until_resumed() -> None:
    async with environment(auto_time_skipping=False) as env:
        handle = await env.start_workflow(Subscription, "cus_4", 999, id="sub-4")
        await _until_registered(handle)
        await handle.signal(Subscription.set_paused, True)
        await env.sleep(timedelta(days=45))
        await asyncio.sleep(0.05)
        assert charges == []
        await handle.signal(Subscription.set_paused, False)
        await _until(lambda: charges == ["sub-4/0"])


async def _until(condition: Any, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():  # noqa: ASYNC110
            await asyncio.sleep(0.005)


async def _until_registered(handle: lw.WorkflowHandle[Any]) -> None:
    async with asyncio.timeout(5):
        while (await handle.describe()).tick_pending:  # noqa: ASYNC110
            await asyncio.sleep(0.005)


async def _until_timer(env: WorkflowEnvironment) -> None:
    async with asyncio.timeout(5):
        while await env.store.next_timer_at() is None:  # noqa: ASYNC110
            await asyncio.sleep(0.005)
