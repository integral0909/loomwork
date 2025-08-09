"""Steps for the encrypted payments example."""

from __future__ import annotations

import loomwork as lw


@lw.step(name="encrypted_payments.charge_card")
async def charge_card(card: str, amount_cents: int) -> dict[str, str | int]:
    """A (simulated) charge. Its arguments and result are payloads like any other, so
    whatever codec chain the workflow's ``DataConverter`` uses applies to them too."""
    charge_id = f"charge-{lw.step_info().idempotency_key}"
    return {"card": card, "amount_cents": amount_cents, "charge_id": charge_id}
