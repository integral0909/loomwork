"""Pydantic models shared by the order fulfillment workflow and its steps.

Kept separate from ``workflows.py`` so that ``steps.py`` can depend on the data shapes
without importing the workflow module itself (steps and workflows both need these
types; the workflow module also needs the steps).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class LineItem(BaseModel):
    sku: str
    quantity: int = Field(gt=0)
    price_cents: int = Field(ge=0)


class Order(BaseModel):
    order_id: str
    customer_id: str
    card: str
    items: list[LineItem]

    @property
    def total_cents(self) -> int:
        return sum(item.quantity * item.price_cents for item in self.items)


class OrderResult(BaseModel):
    order_id: str
    status: str
    reservation_id: str | None = None
    charge_id: str | None = None
    tracking_id: str | None = None
    reason: str | None = None
