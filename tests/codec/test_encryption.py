"""Tests for :class:`EncryptionCodec`."""

from __future__ import annotations

import asyncio
import base64

import pytest
from hypothesis import given
from hypothesis import strategies as st

from loomwork.codec.encryption import ENCODING_ENCRYPTED, EncryptionCodec
from loomwork.codec.errors import IntegrityError, UnknownKeyError
from loomwork.codec.keyring import StaticKeyRing
from loomwork.codec.payload import Payload

KEY_A = b"a" * 32
KEY_B = b"b" * 32


def ring(current: str = "a") -> StaticKeyRing:
    return StaticKeyRing({"a": KEY_A, "b": KEY_B}, current=current)


class TestRoundTrip:
    async def test_encrypt_then_decrypt_restores_original_payload(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"super secret", {"encoding": "json/plain"})
        (encrypted,) = await codec.encode([payload])
        assert encrypted.encoding == ENCODING_ENCRYPTED
        assert encrypted.data != payload.data
        (decrypted,) = await codec.decode([encrypted])
        assert decrypted == payload

    async def test_ciphertext_does_not_contain_plaintext(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"the quick brown fox jumps", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        assert b"quick brown fox" not in encrypted.data

    async def test_metadata_records_key_id_but_not_material(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"x", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        assert encrypted.metadata["codec.encryption.key-id"] == "a"
        b64_a = base64.b64encode(KEY_A).decode()
        assert all(b64_a not in v for v in encrypted.metadata.values())

    async def test_nonces_differ_between_payloads(self) -> None:
        codec = EncryptionCodec(ring())
        payloads = [Payload(b"same bytes", {"encoding": "binary/plain"}) for _ in range(10)]
        encrypted = await codec.encode(payloads)
        nonces = {p.metadata["codec.encryption.nonce"] for p in encrypted}
        ciphertexts = {p.data for p in encrypted}
        assert len(nonces) == len(payloads)
        assert len(ciphertexts) == len(payloads)

    async def test_foreign_payload_passes_through_unchanged(self) -> None:
        codec = EncryptionCodec(ring())
        foreign = Payload(b"plain", {"encoding": "json/plain"})
        (decoded,) = await codec.decode([foreign])
        assert decoded == foreign

    async def test_strict_mode_rejects_unencrypted_payloads(self) -> None:
        codec = EncryptionCodec(ring(), require_encryption=True)
        forged = Payload(b'"attacker"', {"encoding": "json/plain"})
        with pytest.raises(IntegrityError, match="require_encryption"):
            await codec.decode([forged])
        (encrypted,) = await codec.encode([Payload(b"ok", {"encoding": "json/plain"})])
        (decoded,) = await codec.decode([encrypted])
        assert decoded.data == b"ok"

    async def test_empty_payload_round_trips(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"", {"encoding": "binary/null"})
        (encrypted,) = await codec.encode([payload])
        (decrypted,) = await codec.decode([encrypted])
        assert decrypted == payload

    async def test_preserves_unrelated_metadata(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"x", {"encoding": "json/plain", "workflow-id": "wf-1"})
        (encrypted,) = await codec.encode([payload])
        assert encrypted.metadata["workflow-id"] == "wf-1"
        (decrypted,) = await codec.decode([encrypted])
        assert decrypted.metadata["workflow-id"] == "wf-1"


class TestTamperDetection:
    async def test_flipped_ciphertext_bit_raises_integrity_error(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"authenticate me", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        tampered_data = bytearray(encrypted.data)
        tampered_data[0] ^= 0x01
        tampered = Payload(bytes(tampered_data), encrypted.metadata)
        with pytest.raises(IntegrityError, match="authentication"):
            await codec.decode([tampered])

    async def test_modified_metadata_value_raises_integrity_error(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"authenticate me", {"encoding": "binary/plain", "tag": "v1"})
        (encrypted,) = await codec.encode([payload])
        tampered = Payload(encrypted.data, {**encrypted.metadata, "tag": "v2"})
        with pytest.raises(IntegrityError):
            await codec.decode([tampered])

    async def test_added_metadata_key_raises_integrity_error(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"authenticate me", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        tampered = Payload(encrypted.data, {**encrypted.metadata, "injected": "true"})
        with pytest.raises(IntegrityError):
            await codec.decode([tampered])

    async def test_swapped_nonce_raises_integrity_error(self) -> None:
        codec = EncryptionCodec(ring())
        p1 = Payload(b"first message", {"encoding": "binary/plain"})
        p2 = Payload(b"second message", {"encoding": "binary/plain"})
        e1, e2 = await codec.encode([p1, p2])
        swapped = Payload(
            e1.data,
            {**e1.metadata, "codec.encryption.nonce": e2.metadata["codec.encryption.nonce"]},
        )
        with pytest.raises(IntegrityError):
            await codec.decode([swapped])

    async def test_wrong_key_raises_unknown_key_error_not_integrity_error(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"x", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        other_ring = StaticKeyRing({"z": b"z" * 32}, current="z")
        other_codec = EncryptionCodec(other_ring)
        with pytest.raises(UnknownKeyError) as exc_info:
            await other_codec.decode([encrypted])
        assert exc_info.value.key_id == "a"

    async def test_key_that_exists_but_is_wrong_raises_integrity_error(self) -> None:
        """Same key id present on both rings, but the material differs: this can only
        be detected via the authentication tag, not a missing-key lookup."""
        codec = EncryptionCodec(StaticKeyRing({"a": KEY_A}, current="a"))
        payload = Payload(b"x", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        wrong_material_codec = EncryptionCodec(StaticKeyRing({"a": KEY_B}, current="a"))
        with pytest.raises(IntegrityError):
            await wrong_material_codec.decode([encrypted])

    async def test_missing_required_metadata_raises_integrity_error(self) -> None:
        codec = EncryptionCodec(ring())
        payload = Payload(b"x", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        stripped_metadata = dict(encrypted.metadata)
        del stripped_metadata["codec.encryption.nonce"]
        stripped = Payload(encrypted.data, stripped_metadata)
        with pytest.raises(IntegrityError, match="missing"):
            await codec.decode([stripped])


class TestKeyRotation:
    async def test_encoding_after_rotation_uses_new_key(self) -> None:
        base = ring(current="a")
        rotated = base.rotated("c", b"c" * 32)
        codec = EncryptionCodec(rotated)
        payload = Payload(b"x", {"encoding": "binary/plain"})
        (encrypted,) = await codec.encode([payload])
        assert encrypted.metadata["codec.encryption.key-id"] == "c"

    async def test_payloads_encrypted_before_rotation_still_decode(self) -> None:
        base = ring(current="a")
        old_codec = EncryptionCodec(base)
        payload = Payload(b"pre-rotation secret", {"encoding": "binary/plain"})
        (encrypted,) = await old_codec.encode([payload])

        rotated = base.rotated("c", b"c" * 32)
        new_codec = EncryptionCodec(rotated)
        (decrypted,) = await new_codec.decode([encrypted])
        assert decrypted == payload

    async def test_mixed_batch_of_old_and_new_keys_all_decode(self) -> None:
        base = ring(current="a")
        old_codec = EncryptionCodec(base)
        old_payload = Payload(b"old", {"encoding": "binary/plain"})
        (old_encrypted,) = await old_codec.encode([old_payload])

        rotated = base.rotated("c", b"c" * 32)
        new_codec = EncryptionCodec(rotated)
        new_payload = Payload(b"new", {"encoding": "binary/plain"})
        (new_encrypted,) = await new_codec.encode([new_payload])

        decrypted = await new_codec.decode([old_encrypted, new_encrypted])
        assert decrypted == [old_payload, new_payload]


class TestConcurrency:
    async def test_encoding_many_payloads_concurrently_is_safe(self) -> None:
        codec = EncryptionCodec(ring())
        payloads = [
            Payload(f"secret-{i}".encode(), {"encoding": "binary/plain"}) for i in range(100)
        ]

        async def roundtrip(payload: Payload) -> Payload:
            (encrypted,) = await codec.encode([payload])
            (decrypted,) = await codec.decode([encrypted])
            return decrypted

        results = await asyncio.gather(*(roundtrip(p) for p in payloads))
        assert results == payloads


@given(st.binary(max_size=500))
async def test_encryption_roundtrip_property(data: bytes) -> None:
    codec = EncryptionCodec(ring())
    payload = Payload(data, {"encoding": "binary/plain"})
    (encrypted,) = await codec.encode([payload])
    (decrypted,) = await codec.decode([encrypted])
    assert decrypted == payload
