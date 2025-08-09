"""A payment workflow whose arguments, child workflow, signal and result all carry
sensitive card data -- and all of it goes through the codec chain the same way.
"""

from __future__ import annotations

import loomwork as lw
from examples.encrypted_payments.steps import charge_card

#: Confirms the charge, carrying the card number again to show that signals go
#: through the codec chain too, not just the arguments a workflow starts with.
confirm_channel: lw.Channel[str] = lw.Channel("encrypted_payments.confirm", str)


@lw.workflow(name="encrypted_payments.echo_card")
async def echo_card(card: str) -> str:
    """A child workflow, started with the card number as its own argument."""
    return f"child workflow saw {card}"


@lw.workflow(name="encrypted_payments.process_payment")
async def process_payment(card: str, amount_cents: int) -> dict[str, str]:
    receipt = await charge_card(card, amount_cents)
    echoed = await echo_card(card)
    confirmed = await confirm_channel.receive()
    return {
        "charge_id": str(receipt["charge_id"]),
        "card_on_receipt": str(receipt["card"]),
        "child_echo": echoed,
        "confirmed_card": confirmed,
    }
