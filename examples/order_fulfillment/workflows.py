"""The order fulfillment saga.

Reserve inventory, charge the customer, then ship -- unwinding with compensations if
any step fails, and giving the customer a window to cancel before the order ships.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import loomwork as lw
from examples.order_fulfillment.models import Order, OrderResult
from examples.order_fulfillment.steps import (
    charge_payment,
    refund_payment,
    release_inventory,
    reserve_inventory,
    ship_order,
)

#: The customer sends a reason (or an empty string) to cancel before the order ships.
cancel_channel: lw.Channel[str] = lw.Channel("order_fulfillment.cancel", str)

DEFAULT_CANCELLATION_WINDOW = timedelta(hours=2)


@lw.workflow(name="order_fulfillment.process_order")
async def process_order(
    order: Order, cancellation_window: timedelta = DEFAULT_CANCELLATION_WINDOW
) -> OrderResult:
    """Run one order through reservation, payment and shipping.

    ``cancellation_window`` is a workflow argument (rather than a hard-coded constant)
    so a demo can shrink it; the durable wait itself works exactly the same either way,
    since :class:`~loomwork.ManualClock` moves through it instantly.
    """
    stage = "reserving"
    undo = lw.Compensations()

    @lw.query("status")
    def status() -> str:
        return stage

    try:
        reservation_id = await reserve_inventory(order.order_id, order.items)
        undo.add(release_inventory, reservation_id)
        stage = "charging"

        charge_id = await charge_payment(order.card, order.total_cents)
        undo.add(refund_payment, charge_id)
        stage = "awaiting_shipment"

        try:
            async with asyncio.timeout(cancellation_window.total_seconds()):
                reason = await cancel_channel.receive()
        except TimeoutError:
            pass
        else:
            stage = "cancelled"
            await undo.run()
            return OrderResult(
                order_id=order.order_id,
                status="cancelled",
                reservation_id=reservation_id,
                charge_id=charge_id,
                reason=reason or "cancelled by customer",
            )

        stage = "shipping"
        tracking_id = await ship_order(order.order_id)
        stage = "shipped"
        return OrderResult(
            order_id=order.order_id,
            status="shipped",
            reservation_id=reservation_id,
            charge_id=charge_id,
            tracking_id=tracking_id,
        )
    except lw.StepError as exc:
        stage = "failed"
        await undo.run()
        return OrderResult(order_id=order.order_id, status="failed", reason=exc.message)
