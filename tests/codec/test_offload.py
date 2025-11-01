"""Tests for :class:`OffloadCodec`."""

from __future__ import annotations

import asyncio

import pytest

from loomwork.blobs.errors import BlobNotFoundError
from loomwork.blobs.memory import MemoryBlobStore
from loomwork.codec.base import CodecChain
from loomwork.codec.compression import CompressionCodec
from loomwork.codec.encryption import ENCODING_ENCRYPTED, EncryptionCodec
from loomwork.codec.errors import CodecError, IntegrityError
from loomwork.codec.keyring import StaticKeyRing
from loomwork.codec.offload import ENCODING_OFFLOADED, OffloadCodec, offload_key
from loomwork.codec.payload import ENCODING_BYTES, ENCODING_JSON, ENCODING_KEY, Payload

SMALL = b"tiny"
LARGE = b"x" * 10_000


def payload(data: bytes, encoding: str = ENCODING_BYTES) -> Payload:
    return Payload(data, {ENCODING_KEY: encoding})


class TestThreshold:
    async def test_payload_below_threshold_passes_through_unchanged(self) -> None:
        codec = OffloadCodec(MemoryBlobStore(), threshold_bytes=100)
        original = payload(SMALL)
        (encoded,) = await codec.encode([original])
        assert encoded == original

    async def test_payload_at_or_above_threshold_is_offloaded(self) -> None:
        codec = OffloadCodec(MemoryBlobStore(), threshold_bytes=100)
        (encoded,) = await codec.encode([payload(LARGE)])
        assert encoded.encoding == ENCODING_OFFLOADED
        assert len(encoded.data) < 200

    async def test_threshold_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            OffloadCodec(MemoryBlobStore(), threshold_bytes=0)


class TestRoundTrip:
    async def test_decode_restores_the_original_payload(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        original = payload(LARGE)
        (encoded,) = await codec.encode([original])
        (decoded,) = await codec.decode([encoded])
        assert decoded == original

    async def test_decode_of_a_payload_this_codec_never_produced_is_a_passthrough(self) -> None:
        codec = OffloadCodec(MemoryBlobStore(), threshold_bytes=10)
        untouched = payload(SMALL, ENCODING_JSON)
        (decoded,) = await codec.decode([untouched])
        assert decoded == untouched

    async def test_the_blob_store_holds_the_offloaded_bytes(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (encoded,) = await codec.encode([payload(LARGE)])
        key = encoded.metadata["codec.offload.key"]
        assert await store.get(key) == LARGE

    async def test_key_prefix_is_prepended(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10, key_prefix="loomwork/payloads/")
        (encoded,) = await codec.encode([payload(LARGE)])
        key = encoded.metadata["codec.offload.key"]
        assert key.startswith("loomwork/payloads/")
        assert await store.get(key) == LARGE


class TestDedupe:
    async def test_identical_payloads_share_a_key(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (a,) = await codec.encode([payload(LARGE)])
        (b,) = await codec.encode([payload(LARGE)])
        assert a.metadata["codec.offload.key"] == b.metadata["codec.offload.key"]

    async def test_different_payloads_get_different_keys(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (a,) = await codec.encode([payload(LARGE)])
        (b,) = await codec.encode([payload(LARGE + b"!")])
        assert a.metadata["codec.offload.key"] != b.metadata["codec.offload.key"]

    async def test_reoffloading_an_existing_key_does_not_overwrite_it(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (first,) = await codec.encode([payload(LARGE)])
        key = first.metadata["codec.offload.key"]
        # A concurrent or retried encode of the same content is always safe: content
        # addressing means it is, by construction, writing the same bytes again.
        (second,) = await codec.encode([payload(LARGE)])
        assert second.metadata["codec.offload.key"] == key
        assert await store.get(key) == LARGE

    async def test_concurrent_encodes_of_the_same_payload_are_safe(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        results = await asyncio.gather(*(codec.encode([payload(LARGE)]) for _ in range(8)))
        keys = {r[0].metadata["codec.offload.key"] for r in results}
        assert len(keys) == 1
        assert await store.get(keys.pop()) == LARGE


class TestErrors:
    async def test_missing_blob_raises_codec_error_naming_the_key(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (encoded,) = await codec.encode([payload(LARGE)])
        key = encoded.metadata["codec.offload.key"]
        await store.delete(key)

        with pytest.raises(CodecError) as exc_info:
            await codec.decode([encoded])
        assert key in str(exc_info.value)
        # A missing blob is a codec-level failure with context, not a bare store error.
        assert not isinstance(exc_info.value, BlobNotFoundError)

    async def test_corrupted_blob_raises_integrity_error(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (encoded,) = await codec.encode([payload(LARGE)])
        key = encoded.metadata["codec.offload.key"]
        await store.put(key, b"not the original bytes at all")

        with pytest.raises(IntegrityError):
            await codec.decode([encoded])

    async def test_missing_reference_metadata_raises_integrity_error(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (encoded,) = await codec.encode([payload(LARGE)])
        stripped = Payload(
            encoded.data,
            {k: v for k, v in encoded.metadata.items() if k != "codec.offload.key"},
        )
        with pytest.raises(IntegrityError):
            await codec.decode([stripped])


class TestOffloadKeyHelper:
    async def test_returns_the_key_for_an_offloaded_payload(self) -> None:
        store = MemoryBlobStore()
        codec = OffloadCodec(store, threshold_bytes=10)
        (encoded,) = await codec.encode([payload(LARGE)])
        assert offload_key(encoded) == encoded.metadata["codec.offload.key"]

    async def test_returns_none_for_a_payload_this_codec_did_not_produce(self) -> None:
        assert offload_key(payload(SMALL)) is None


class TestCompositionWithCompressionAndEncryption:
    """OffloadCodec must be outermost: after compression and encryption on encode, so
    the blob store only ever holds ciphertext."""

    SECRET = b"the card number is 4242-4242-4242-4242, do not leak it"

    def full_chain(self, store: MemoryBlobStore) -> CodecChain:
        keyring = StaticKeyRing({"k1": b"k" * 32}, current="k1")
        return CodecChain(
            [
                CompressionCodec(threshold=16),
                EncryptionCodec(keyring),
                OffloadCodec(store, threshold_bytes=32),
            ]
        )

    async def test_round_trips_through_the_full_chain(self) -> None:
        store = MemoryBlobStore()
        chain = self.full_chain(store)
        original = payload(self.SECRET * 50)  # large enough to clear the offload threshold
        (encoded,) = await chain.encode([original])
        (decoded,) = await chain.decode([encoded])
        assert decoded == original

    async def test_the_reference_payload_is_small(self) -> None:
        store = MemoryBlobStore()
        chain = self.full_chain(store)
        (encoded,) = await chain.encode([payload(self.SECRET * 50)])
        assert encoded.encoding == ENCODING_OFFLOADED
        assert len(encoded.data) < 64

    async def test_the_stored_blob_is_ciphertext_not_plaintext(self) -> None:
        store = MemoryBlobStore()
        chain = self.full_chain(store)
        (encoded,) = await chain.encode([payload(self.SECRET * 50)])
        key = encoded.metadata["codec.offload.key"]
        blob_bytes = await store.get(key)
        assert self.SECRET not in blob_bytes
        assert b"4242-4242-4242-4242" not in blob_bytes

    async def test_decoding_one_codec_at_a_time_matches_the_chain(self) -> None:
        """The blob holds exactly what ``EncryptionCodec`` produced: decrypting it
        directly (bypassing ``OffloadCodec``) recovers the same plaintext as the full
        chain does, proving offloading is transparent to the layers underneath it."""
        store = MemoryBlobStore()
        keyring = StaticKeyRing({"k1": b"k" * 32}, current="k1")
        compression = CompressionCodec(threshold=16)
        encryption = EncryptionCodec(keyring)
        offload = OffloadCodec(store, threshold_bytes=32)
        original = payload(self.SECRET * 50)

        (compressed,) = await compression.encode([original])
        (encrypted,) = await encryption.encode([compressed])
        (offloaded,) = await offload.encode([encrypted])

        key = offloaded.metadata["codec.offload.key"]
        blob_bytes = await store.get(key)
        assert blob_bytes == encrypted.data
        assert offloaded.metadata["codec.offload.inner-encoding"] == ENCODING_ENCRYPTED
