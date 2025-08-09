"""Run the order fulfillment saga through three scenarios.

.. code-block:: console

    uv run python -m examples.order_fulfillment.main

See ``README.md`` for what each scenario demonstrates.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import loomwork as lw
from examples.order_fulfillment import steps
from examples.order_fulfillment.models import LineItem, Order, OrderResult
from examples.order_fulfillment.steps import (
    carrier,
    charge_payment,
    gateway,
    release_inventory,
    reserve_inventory,
    ship_order,
    warehouse,
)
from examples.order_fulfillment.workflows import cancel_channel, process_order
from examples.support import DEMO_POLL_INTERVAL, rule, wait_for_query
from loomwork import ManualClock, MemoryStore
from loomwork.engine.config import EngineConfig
from loomwork.worker import WorkerOptions

_STEPS: list[lw.StepDefinition[..., Any]] = [
    reserve_inventory,
    release_inventory,
    charge_payment,
    steps.refund_payment,
    ship_order,
]


def _order(order_id: str, card: str) -> Order:
    return Order(
        order_id=order_id,
        customer_id=f"cust-{order_id}",
        card=card,
        items=[LineItem(sku="widget", quantity=2, price_cents=1999)],
    )


async def _await_past_payment(clock: ManualClock, handle: lw.WorkflowHandle[OrderResult]) -> str:
    """Advance the clock through the flaky gateway's retry backoff and return the
    workflow's stage once it is past reserving and charging."""
    stage = await wait_for_query(
        clock, handle, "status", lambda s: s not in ("reserving", "charging")
    )
    return str(stage)


async def main() -> None:
    clock = ManualClock()
    app = lw.Loomwork(
        store=MemoryStore(),
        workflows=[process_order],
        steps=_STEPS,
        clock=clock,
        config=EngineConfig(poll_interval=DEMO_POLL_INTERVAL),
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with app:
        print(rule("Scenario 1: happy path (payment gateway is flaky but recovers)"))
        happy = await app.start_workflow(process_order, _order("A-1001", "4242-4242-4242-4242"))
        stage = await _await_past_payment(clock, happy)
        print(f"order-A-1001 reached stage {stage!r} after the gateway's retries settled")
        print("the customer lets the cancellation window pass without acting")
        await clock.advance(timedelta(hours=3))
        happy_result = await happy.result(timeout=5)
        print(f"result: {happy_result.model_dump()}")

        print(rule("Scenario 2: customer cancels before shipping"))
        cancelled = await app.start_workflow(process_order, _order("A-1002", "4242-4242-4242-4242"))
        stage = await _await_past_payment(clock, cancelled)
        print(f"order-A-1002 reached stage {stage!r}; the customer changes their mind")
        await cancelled.signal(cancel_channel, "found it cheaper elsewhere")
        cancelled_result = await cancelled.result(timeout=5)
        print(f"result: {cancelled_result.model_dump()}")

        print(rule("Scenario 3: card is declined (non-retryable failure)"))
        declined = await app.start_workflow(process_order, _order("A-1003", "4000-0000-0000-0002"))
        declined_result = await declined.result(timeout=5)
        print(f"result: {declined_result.model_dump()}")

    print(rule("Downstream system state after all three orders"))
    print(f"warehouse reservations still held: {sorted(warehouse.reservations)}")
    print(f"warehouse releases (compensations ran): {sorted(warehouse.released)}")
    print(f"gateway charges: {sorted(gateway.charges.values())}")
    print(f"gateway refunds (compensations ran): {sorted(gateway.refunded)}")
    print(f"carrier shipments: {sorted(carrier.shipped)}")


if __name__ == "__main__":
    asyncio.run(main())
