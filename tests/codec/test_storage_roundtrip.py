"""End-to-end tests simulating what actually happens around a store.

The store never sees a :class:`Payload` object directly -- it sees whatever
``Payload.to_dict()`` / ``Payload.from_dict()`` produce (plain JSON-compatible dicts, the
same shape ``loomwork.history.serde`` uses to persist events). These tests exercise the
codec chain across that boundary, and also cover the case the security model calls out
explicitly: a workflow failure's ``details`` traveling through the same converter and
codec chain as ordinary arguments and results.
"""

from __future__ import annotations

import json

from loomwork.codec.base import CodecChain
from loomwork.codec.compression import CompressionCodec
from loomwork.codec.converter import DataConverter
from loomwork.codec.encryption import EncryptionCodec
from loomwork.codec.keyring import StaticKeyRing
from loomwork.codec.limits import PayloadSizeLimitCodec
from loomwork.codec.payload import ENCODING_JSON, Payload
from loomwork.failure import FailureInfo

KEY = b"s" * 32


def full_chain() -> CodecChain:
    return CodecChain(
        [
            CompressionCodec(threshold=10),
            EncryptionCodec(StaticKeyRing({"s1": KEY}, current="s1")),
            PayloadSizeLimitCodec(max_bytes=1_000_000),
        ]
    )


class TestPayloadDictRoundtrip:
    """Simulates what a store implementation actually persists and reloads."""

    async def test_encrypted_payload_survives_a_json_store_round_trip(self) -> None:
        converter = DataConverter(codec=full_chain())
        value = {"card_number": "4242-4242-4242-4242", "amount_cents": 1999}
        payload = await converter.encode_async(value)

        # What a store would actually write: a JSON-serializable dict, then read back.
        stored_json = json.dumps(payload.to_dict())
        reloaded_payload = Payload.from_dict(json.loads(stored_json))

        assert reloaded_payload == payload
        assert await converter.decode_async(reloaded_payload) == value

    async def test_stored_json_does_not_contain_the_plaintext(self) -> None:
        converter = DataConverter(codec=full_chain())
        payload = await converter.encode_async({"secret": "do-not-leak-this-string"})
        stored_json = json.dumps(payload.to_dict())
        assert "do-not-leak-this-string" not in stored_json

    async def test_history_recorded_without_a_codec_still_replays_after_one_is_added(
        self,
    ) -> None:
        """The scenario SECURITY.md and the module docs both call out: turning on
        encryption for an existing deployment must not break replay of old history."""
        original_converter = DataConverter()
        value = {"order_id": "o-1"}
        payload = original_converter.encode(value)
        stored_json = json.dumps(payload.to_dict())

        upgraded_converter = DataConverter(codec=full_chain())
        reloaded = Payload.from_dict(json.loads(stored_json))
        assert reloaded.encoding == ENCODING_JSON  # untouched: no codec wrote it
        assert await upgraded_converter.decode_async(reloaded) == value


class TestFailureDetailsThroughTheCodecChain:
    """``FailureInfo.details`` is user data and goes through the same converter and
    codec chain as arguments and results; only ``message`` and ``stack_trace`` are
    always stored in the clear (see docs/payload-encryption.md)."""

    async def test_failure_details_round_trip_encrypted(self) -> None:
        converter = DataConverter(codec=full_chain())
        failure = FailureInfo(
            message="card declined",
            type="CardDeclined",
            details={"card_last4": "4242", "reason_code": "insufficient_funds"},
        )
        payload = await converter.encode_async(failure.details)
        assert payload.encoding not in (ENCODING_JSON, "")
        assert await converter.decode_async(payload) == failure.details

    async def test_failure_details_ciphertext_does_not_leak_sensitive_values(self) -> None:
        converter = DataConverter(codec=full_chain())
        failure = FailureInfo(
            message="payment failed",
            type="PaymentError",
            details={"account_iban": "DE89370400440532013000"},
        )
        payload = await converter.encode_async(failure.details)
        assert b"DE89370400440532013000" not in payload.data

    async def test_reconstructed_exception_carries_decoded_details(self) -> None:
        converter = DataConverter(codec=full_chain())
        failure = FailureInfo(message="bad input", type="ValidationError", details={"field": "x"})
        payload = await converter.encode_async(failure.details)
        decoded_details = await converter.decode_async(payload)
        rebuilt = FailureInfo(message=failure.message, type=failure.type, details=decoded_details)
        exc = rebuilt.to_exception()
        assert exc.details == {"field": "x"}
