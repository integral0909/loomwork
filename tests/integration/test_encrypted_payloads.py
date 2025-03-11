"""End-to-end payload encryption: the store never sees plaintext."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

import loomwork as lw
from loomwork.codec import CodecChain, CompressionCodec, DataConverter, StaticKeyRing
from loomwork.codec.encryption import EncryptionCodec
from loomwork.codec.errors import IntegrityError
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration

SECRET = "4111-1111-1111-1111"
card_channel = lw.Channel("card", str)


@lw.step(name="enc.charge")
async def charge(card: str) -> dict[str, Any]:
    assert card == SECRET
    await lw.heartbeat({"checkpoint": card[-4:]})
    return {"charged": card, "amount": 1999}


@lw.workflow(name="enc.child")
async def child_wf(card: str) -> str:
    return f"child saw {card}"


@lw.workflow(name="enc.payment")
async def payment(card: str) -> dict[str, Any]:
    receipt = await charge(card)
    echoed = await child_wf(card)
    second = await card_channel.receive()

    @lw.update("peek")
    def peek(tag: str) -> str:
        return f"{tag}:{receipt['charged']}"

    await lw.sleep(timedelta(seconds=1))
    return {"receipt": receipt, "child": echoed, "signal": second}


def converter(key: bytes, *, strict: bool = False) -> DataConverter:
    ring = StaticKeyRing({"k1": key}, current="k1")
    return DataConverter(
        codec=CodecChain(
            [CompressionCodec(threshold=16), EncryptionCodec(ring, require_encryption=strict)]
        )
    )


async def test_store_holds_only_ciphertext(tmp_path: Path, clock: lw.ManualClock) -> None:
    path = tmp_path / "enc.db"
    key = b"k" * 32
    async with SQLiteStore(path) as store:
        app = lw.Loomwork(
            store=store,
            workflows=[payment, child_wf],
            steps=[charge],
            clock=clock,
            converter=converter(key),
            config=FAST_CONFIG,
            worker_options=WorkerOptions(shutdown_grace=0.2),
        )
        async with app:
            handle = await app.start_workflow(payment, SECRET)
            await handle.signal(card_channel, SECRET)

            async def timer_armed() -> bool:
                return await store.next_timer_at() is not None

            await eventually(timer_armed, timeout=5)
            await clock.advance(2)
            result = await handle.result(timeout=10)
    assert result == {
        "receipt": {"charged": SECRET, "amount": 1999},
        "child": f"child saw {SECRET}",
        "signal": SECRET,
    }

    raw = path.read_bytes()
    for suffix in ("", "-wal"):
        extra = tmp_path / f"enc.db{suffix}"
        if extra.exists():
            raw += extra.read_bytes()
    assert SECRET.encode() not in raw
    assert b"1111-1111" not in raw
    assert b"binary/encrypted" in raw

    # Without the key the history cannot be read; with a wrong key it is rejected.
    async with SQLiteStore(path) as store:
        history = await store.load_history(handle.run_id or "")
        started = history[0]
        payload = started.args[0]  # type: ignore[attr-defined]
        assert payload.encoding == "binary/encrypted"
        wrong = converter(b"x" * 32)
        with pytest.raises(IntegrityError):
            await wrong.decode_async(payload)
        assert await converter(key).decode_async(payload) == SECRET


async def test_strict_mode_rejects_plaintext_history(tmp_path: Path, clock: lw.ManualClock) -> None:
    @lw.workflow(name="enc.plain")
    async def plain(value: str) -> str:
        return value

    async with SQLiteStore(tmp_path / "strict.db") as store:
        client_only = lw.Loomwork(store=store, clock=clock, run_worker=False)
        async with client_only:
            handle = await client_only.start_workflow("enc.plain", "not encrypted")
        app = lw.Loomwork(
            store=store,
            workflows=[plain],
            clock=clock,
            converter=converter(b"k" * 32, strict=True),
            config=replace(FAST_CONFIG, poll_interval=0.02),
            worker_options=WorkerOptions(shutdown_grace=0.2),
        )
        async with app:

            async def tick_failed() -> bool:
                record = await store.get_execution(handle.run_id or "")
                return "IntegrityError" in (record.last_tick_error or "")

            await eventually(tick_failed)
        record = await store.get_execution(handle.run_id or "")
        assert record.status is lw.ExecutionStatus.RUNNING
        assert json.dumps(record.last_tick_error).count("require_encryption") == 1
