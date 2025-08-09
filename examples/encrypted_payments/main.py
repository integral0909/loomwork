"""Process payments with payload encryption, and rotate keys without downtime.

.. code-block:: console

    uv run python -m examples.encrypted_payments.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import loomwork as lw
from examples.encrypted_payments.steps import charge_card
from examples.encrypted_payments.workflows import confirm_channel, echo_card, process_payment
from examples.support import rule
from loomwork.codec import CodecChain, CompressionCodec, DataConverter, StaticKeyRing
from loomwork.codec.encryption import EncryptionCodec
from loomwork.codec.errors import UnknownKeyError
from loomwork.codec.keys import generate_key
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions

CARD_1 = "4242-4242-4242-4242"
CARD_2 = "4111-1111-1111-1111"

_WORKFLOWS: list[lw.WorkflowDefinition[..., Any]] = [process_payment, echo_card]
_STEPS: list[lw.StepDefinition[..., Any]] = [charge_card]


def _converter(ring: StaticKeyRing) -> DataConverter:
    return DataConverter(codec=CodecChain([CompressionCodec(threshold=64), EncryptionCodec(ring)]))


async def main() -> None:
    key_q1 = generate_key()  # generated at runtime; never committed or logged
    ring_q1 = StaticKeyRing({"2024-q1": key_q1}, current="2024-q1")

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "payments.db"
        async with SQLiteStore(db_path) as store:
            print(rule("Processing a payment, encrypted under key 2024-q1"))
            async with lw.Loomwork(
                store=store,
                workflows=_WORKFLOWS,
                steps=_STEPS,
                converter=_converter(ring_q1),
                worker_options=WorkerOptions(shutdown_grace=0.2),
            ) as app_q1:
                handle = await app_q1.start_workflow(process_payment, CARD_1, 4999, id="payment-1")
                await handle.signal(confirm_channel, CARD_1)
                result = await handle.result(timeout=5)
                print(f"result: {result}")

            print(rule("Inspecting the store file directly"))
            raw = db_path.read_bytes()
            for suffix in ("-wal", "-shm"):
                sidecar = db_path.with_name(db_path.name + suffix)
                if sidecar.exists():
                    raw += sidecar.read_bytes()
            print(f"the card number appears in the raw file bytes: {CARD_1.encode() in raw}")
            print(f"an encryption marker appears instead: {b'binary/encrypted' in raw}")

            print(rule("Rotating to key 2024-q2: new payloads use it, old ones still decode"))
            key_q2 = generate_key()
            ring_q2 = ring_q1.rotated("2024-q2", key_q2)  # keeps 2024-q1 resolvable too
            async with lw.Loomwork(
                store=store,
                workflows=_WORKFLOWS,
                steps=_STEPS,
                converter=_converter(ring_q2),
                worker_options=WorkerOptions(shutdown_grace=0.2),
            ) as app_q2:
                handle_2 = await app_q2.start_workflow(
                    process_payment, CARD_2, 1500, id="payment-2"
                )
                await handle_2.signal(confirm_channel, CARD_2)
                result_2 = await handle_2.result(timeout=5)
                print(f"payment-2 (encrypted under 2024-q2): {result_2}")

                # payment-1's history was encrypted under 2024-q1; the rotated ring still
                # knows that key, so it keeps decoding without any migration step.
                payment_1_again = app_q2.get_handle(workflow_id="payment-1")
                replayed = await payment_1_again.result(timeout=5)
                print(f"payment-1, read back through the rotated ring: {replayed}")

            print(rule("Retiring key 2024-q1 before every run using it has closed"))
            retired_ring = StaticKeyRing({"2024-q2": key_q2}, current="2024-q2")
            async with lw.Loomwork(
                store=store, converter=_converter(retired_ring), run_worker=False
            ) as read_only:
                try:
                    await read_only.get_handle(workflow_id="payment-1").result(timeout=5)
                except UnknownKeyError as exc:
                    print(f"payment-1 can no longer be read: {exc}")
                else:
                    raise AssertionError("expected UnknownKeyError")


if __name__ == "__main__":
    asyncio.run(main())
