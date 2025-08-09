"""Tests for ``examples.order_fulfillment``."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

import loomwork as lw
from examples.order_fulfillment import steps
from examples.order_fulfillment.models import LineItem, Order
from examples.order_fulfillment.workflows import cancel_channel, process_order
from examples.support import wait_for_query

if TYPE_CHECKING:
    from loomwork import ManualClock
    from tests.conftest import AppFactory

STEP_DEFS = [
    steps.reserve_inventory,
    steps.release_inventory,
    steps.charge_payment,
    steps.refund_payment,
    steps.ship_order,
]


def _order(order_id: str, card: str) -> Order:
    return Order(
        order_id=order_id,
        customer_id=f"cust-{order_id}",
        card=card,
        items=[LineItem(sku="widget", quantity=2, price_cents=500)],
    )


@pytest.fixture(autouse=True)
def _reset_simulated_systems() -> None:
    steps.warehouse.reservations.clear()
    steps.warehouse.released.clear()
    steps.gateway.attempts.clear()
    steps.gateway.charges.clear()
    steps.gateway.amounts_cents.clear()
    steps.gateway.refunded.clear()
    steps.carrier.shipped.clear()


async def test_happy_path_ships_after_gateway_retries(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([process_order], STEP_DEFS)
    handle = await app.start_workflow(process_order, _order("ord-1", "4242-4242-4242-4242"))

    stage = await wait_for_query(
        clock, handle, "status", lambda s: s not in ("reserving", "charging")
    )
    assert stage == "awaiting_shipment"
    # The gateway fails the first two attempts for a new idempotency key.
    assert steps.gateway.attempts == {next(iter(steps.gateway.attempts)): 3}

    await clock.advance(timedelta(hours=3))
    result = await handle.result(timeout=5)

    assert result.status == "shipped"
    assert result.tracking_id == "track-ord-1"
    assert steps.carrier.shipped == ["ord-1"]
    assert steps.warehouse.released == []  # nothing was rolled back
    assert steps.gateway.refunded == []


async def test_customer_cancels_before_shipping(make_app: AppFactory, clock: ManualClock) -> None:
    app = await make_app([process_order], STEP_DEFS)
    handle = await app.start_workflow(process_order, _order("ord-2", "4242-4242-4242-4242"))

    await wait_for_query(clock, handle, "status", lambda s: s == "awaiting_shipment")
    await handle.signal(cancel_channel, "changed my mind")
    result = await handle.result(timeout=5)

    assert result.status == "cancelled"
    assert result.reason == "changed my mind"
    assert steps.carrier.shipped == []
    assert steps.warehouse.released == ["res-ord-2"]
    assert len(steps.gateway.refunded) == 1


async def test_declined_card_rolls_back_the_reservation(
    make_app: AppFactory, clock: ManualClock
) -> None:
    app = await make_app([process_order], STEP_DEFS)
    handle = await app.start_workflow(process_order, _order("ord-3", "4000-0000-0000-0002"))
    result = await handle.result(timeout=5)

    assert result.status == "failed"
    assert "card declined" in (result.reason or "")
    assert steps.warehouse.released == ["res-ord-3"]
    assert steps.gateway.charges == {}  # the charge never succeeded
    assert steps.gateway.refunded == []  # so there was nothing to refund
    assert steps.carrier.shipped == []


async def test_cancellation_window_is_configurable(
    make_app: AppFactory, clock: ManualClock
) -> None:
    """A shorter window still behaves the same way, just sooner."""
    app = await make_app([process_order], STEP_DEFS)
    handle = await app.start_workflow(
        process_order, _order("ord-4", "4242-4242-4242-4242"), timedelta(minutes=5)
    )
    await wait_for_query(clock, handle, "status", lambda s: s == "awaiting_shipment")
    await clock.advance(timedelta(minutes=6))
    result = await handle.result(timeout=5)
    assert result.status == "shipped"


def test_gateway_never_charges_the_same_idempotency_key_twice() -> None:
    """The step's retry relies on this: a retried attempt that reaches the (simulated)
    gateway again, after an earlier attempt actually succeeded, must not charge twice."""
    gateway = steps.PaymentGateway()
    key = "run-1:2"

    for _ in range(2):  # the gateway fails the first two attempts for any new key
        with pytest.raises(lw.ApplicationError, match="gateway timed out"):
            gateway.charge(key, "4242-4242-4242-4242", 999)
    charge_id = gateway.charge(key, "4242-4242-4242-4242", 999)  # third attempt succeeds
    replayed = gateway.charge(key, "4242-4242-4242-4242", 999)  # a retry after success

    assert charge_id == replayed
    assert gateway.attempts[key] == 3  # the replay did not count as a fourth attempt
    assert gateway.amounts_cents[charge_id] == 999

    # A different idempotency key starts its own attempt count from scratch.
    with pytest.raises(lw.ApplicationError, match="gateway timed out"):
        gateway.charge("run-2:2", "4242-4242-4242-4242", 500)
    assert gateway.attempts["run-2:2"] == 1


def test_gateway_rejects_a_declined_card_without_retrying() -> None:
    gateway = steps.PaymentGateway()
    with pytest.raises(lw.ApplicationError, match="card declined") as excinfo:
        gateway.charge("run-3:2", "4000-0000-0000-0002", 100)
    assert excinfo.value.non_retryable is True
    assert gateway.charges == {}
