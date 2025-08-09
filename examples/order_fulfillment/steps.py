"""Steps for the order fulfillment saga.

Each step is a small wrapper around a (simulated) external system: a warehouse
inventory service, a payment gateway and a shipping carrier. Real integrations would
call out over the network here; these stand-ins let the example run without any
external dependency while still exercising retries, idempotency and compensation.
"""

from __future__ import annotations

import loomwork as lw
from examples.order_fulfillment.models import LineItem

#: Card numbers that the simulated gateway always declines, no matter how many times
#: it is retried -- modeling a business-level rejection rather than a transient outage.
DECLINED_CARDS = frozenset({"4000-0000-0000-0002"})


class Warehouse:
    """A toy warehouse: tracks reservations so they can be released on rollback."""

    def __init__(self) -> None:
        self.reservations: dict[str, list[LineItem]] = {}
        self.released: list[str] = []

    def reserve(self, order_id: str, items: list[LineItem]) -> str:
        reservation_id = f"res-{order_id}"
        self.reservations[reservation_id] = items
        return reservation_id

    def release(self, reservation_id: str) -> None:
        self.reservations.pop(reservation_id, None)
        self.released.append(reservation_id)


class PaymentGateway:
    """A flaky payment gateway.

    The first two attempts for a given idempotency key fail with a transient error; the
    third succeeds -- unless the card is on the decline list, in which case every
    attempt fails with a non-retryable error. Charges are keyed by the step's
    idempotency key so a retried attempt that actually reached the (simulated) gateway
    before failing to report back never charges the customer twice.
    """

    def __init__(self) -> None:
        self.attempts: dict[str, int] = {}
        self.charges: dict[str, str] = {}
        self.amounts_cents: dict[str, int] = {}
        self.refunded: list[str] = []

    def charge(self, idempotency_key: str, card: str, amount_cents: int) -> str:
        if idempotency_key in self.charges:
            return self.charges[idempotency_key]  # already charged; replay the result
        if card in DECLINED_CARDS:
            raise lw.ApplicationError("card declined", type="CardDeclined", non_retryable=True)
        attempt = self.attempts.get(idempotency_key, 0) + 1
        self.attempts[idempotency_key] = attempt
        if attempt < 3:
            raise lw.ApplicationError("gateway timed out", type="GatewayTimeout")
        charge_id = f"charge-{idempotency_key}"
        self.charges[idempotency_key] = charge_id
        self.amounts_cents[charge_id] = amount_cents
        return charge_id

    def refund(self, charge_id: str) -> None:
        self.refunded.append(charge_id)


class Carrier:
    """A shipping carrier that hands back a tracking number."""

    def __init__(self) -> None:
        self.shipped: list[str] = []

    def ship(self, order_id: str) -> str:
        self.shipped.append(order_id)
        return f"track-{order_id}"


#: Module-level instances stand in for connections to external systems that a real
#: deployment would construct once per worker process and inject; steps close over
#: them the same way real steps close over an HTTP client or a database pool.
warehouse = Warehouse()
gateway = PaymentGateway()
carrier = Carrier()


@lw.step(name="order_fulfillment.reserve_inventory")
async def reserve_inventory(order_id: str, items: list[LineItem]) -> str:
    """Reserve stock for ``items`` and return a reservation id."""
    return warehouse.reserve(order_id, items)


@lw.step(name="order_fulfillment.release_inventory")
async def release_inventory(reservation_id: str) -> None:
    """Compensation for :func:`reserve_inventory`: release a reservation."""
    warehouse.release(reservation_id)


@lw.step(
    name="order_fulfillment.charge_payment",
    retry=lw.RetryPolicy(initial_interval=0.01, max_interval=0.05, max_attempts=5, jitter=0),
)
async def charge_payment(card: str, amount_cents: int) -> str:
    """Charge ``card``, using the step's idempotency key to avoid a double charge.

    :func:`loomwork.step_info` is only valid while a step attempt is executing, which
    is exactly when this function runs; the key is stable across every retry of the
    *same* scheduled step, so a retried attempt that reaches the gateway a second time
    (because the first attempt's response was lost, not because it failed) still maps
    to one charge.
    """
    key = lw.step_info().idempotency_key
    return gateway.charge(key, card, amount_cents)


@lw.step(name="order_fulfillment.refund_payment")
async def refund_payment(charge_id: str) -> None:
    """Compensation for :func:`charge_payment`: refund a completed charge."""
    gateway.refund(charge_id)


@lw.step(name="order_fulfillment.ship_order")
async def ship_order(order_id: str) -> str:
    """Hand the order to the carrier and return a tracking number."""
    return carrier.ship(order_id)
