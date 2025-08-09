"""Tests for ``examples.encrypted_payments``."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

import loomwork as lw
from examples.encrypted_payments.steps import charge_card
from examples.encrypted_payments.workflows import confirm_channel, echo_card, process_payment
from loomwork.codec import CodecChain, CompressionCodec, DataConverter, StaticKeyRing
from loomwork.codec.encryption import EncryptionCodec
from loomwork.codec.errors import UnknownKeyError
from loomwork.codec.keys import generate_key
from loomwork.store.base import Store
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions

CARD = "4242-4242-4242-4242"
WORKFLOWS: list[lw.WorkflowDefinition[..., Any]] = [process_payment, echo_card]
STEP_DEFS: list[lw.StepDefinition[..., Any]] = [charge_card]


def _converter(ring: StaticKeyRing) -> DataConverter:
    """The same codec chain ``examples.encrypted_payments.main`` builds."""
    return DataConverter(codec=CodecChain([CompressionCodec(threshold=64), EncryptionCodec(ring)]))


async def _process(store: Store, converter: lw.DataConverter, card: str, workflow_id: str) -> Any:
    async with lw.Loomwork(
        store=store,
        workflows=WORKFLOWS,
        steps=STEP_DEFS,
        converter=converter,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    ) as app:
        handle = await app.start_workflow(process_payment, card, 999, id=workflow_id)
        await handle.signal(confirm_channel, card)
        return await handle.result(timeout=5)


async def test_payment_round_trips_under_encryption(tmp_path: Path) -> None:
    ring = StaticKeyRing({"k1": generate_key()}, current="k1")
    async with SQLiteStore(tmp_path / "payments.db") as store:
        result = await _process(store, _converter(ring), CARD, "payment-1")

    assert result["card_on_receipt"] == CARD
    assert result["child_echo"] == f"child workflow saw {CARD}"
    assert result["confirmed_card"] == CARD


async def test_store_file_never_contains_the_card_number(tmp_path: Path) -> None:
    ring = StaticKeyRing({"k1": generate_key()}, current="k1")
    db_path = tmp_path / "payments.db"
    async with SQLiteStore(db_path) as store:
        await _process(store, _converter(ring), CARD, "payment-1")

    raw = db_path.read_bytes()
    for suffix in ("-wal", "-shm"):
        sidecar = db_path.with_name(db_path.name + suffix)
        if sidecar.exists():
            raw += sidecar.read_bytes()
    assert CARD.encode() not in raw
    assert b"binary/encrypted" in raw


async def test_rotated_ring_still_decodes_history_written_under_the_old_key(
    tmp_path: Path,
) -> None:
    key_1 = generate_key()
    ring_1 = StaticKeyRing({"k1": key_1}, current="k1")
    async with SQLiteStore(tmp_path / "payments.db") as store:
        await _process(store, _converter(ring_1), CARD, "payment-1")

        key_2 = generate_key()
        ring_2 = ring_1.rotated("k2", key_2)
        rotated_app = lw.Loomwork(store=store, converter=_converter(ring_2), run_worker=False)
        async with rotated_app:
            result = await rotated_app.get_handle(workflow_id="payment-1").result(timeout=5)
        assert result["card_on_receipt"] == CARD


async def test_retiring_the_old_key_makes_its_history_unreadable(tmp_path: Path) -> None:
    key_1 = generate_key()
    ring_1 = StaticKeyRing({"k1": key_1}, current="k1")
    async with SQLiteStore(tmp_path / "payments.db") as store:
        await _process(store, _converter(ring_1), CARD, "payment-1")

        retired_ring = StaticKeyRing({"k2": generate_key()}, current="k2")
        retired_app = lw.Loomwork(store=store, converter=_converter(retired_ring), run_worker=False)
        async with retired_app:
            with pytest.raises(UnknownKeyError):
                await retired_app.get_handle(workflow_id="payment-1").result(timeout=5)


async def test_strict_mode_rejects_a_history_recorded_without_encryption(
    tmp_path: Path,
) -> None:
    """``require_encryption=True`` is the downgrade protection from
    ``docs/payload-encryption.md``: once enabled, a payload that isn't AES-GCM
    ciphertext -- including one written before encryption was ever turned on -- is
    rejected instead of silently passed through."""

    @lw.workflow(name="encrypted_payments.unencrypted_echo")
    async def unencrypted_echo(card: str) -> str:
        return card

    async with SQLiteStore(tmp_path / "strict.db") as store:
        client_only = lw.Loomwork(store=store, run_worker=False)
        async with client_only:
            handle = await client_only.start_workflow("encrypted_payments.unencrypted_echo", CARD)

        strict_ring = StaticKeyRing({"k1": generate_key()}, current="k1")
        strict_converter = DataConverter(
            codec=CodecChain(
                [
                    CompressionCodec(threshold=64),
                    EncryptionCodec(strict_ring, require_encryption=True),
                ]
            )
        )
        app = lw.Loomwork(
            store=store,
            workflows=[unencrypted_echo],
            converter=strict_converter,
            worker_options=WorkerOptions(shutdown_grace=0.2),
        )
        async with app:
            for _ in range(50):
                record = await store.get_execution(handle.run_id or "")
                if "IntegrityError" in (record.last_tick_error or ""):
                    break
                await asyncio.sleep(0.02)
            else:
                pytest.fail("tick never failed with an IntegrityError")
            assert (
                record.status is lw.ExecutionStatus.RUNNING
            )  # the tick retries; the run doesn't fail
