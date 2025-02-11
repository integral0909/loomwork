"""Tests for :class:`CompressionCodec`."""

from __future__ import annotations

import zlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from loomwork.codec.compression import (
    ENCODING_COMPRESSED,
    CompressionAlgorithm,
    CompressionCodec,
    ZlibAlgorithm,
    get_algorithm,
    register_algorithm,
)
from loomwork.codec.errors import CodecError
from loomwork.codec.payload import Payload

COMPRESSIBLE = b"x" * 5000  # highly repetitive: compresses very well
TINY = b"hi"


class TestThresholdAndRatio:
    async def test_large_compressible_payload_is_compressed(self) -> None:
        codec = CompressionCodec(threshold=100)
        payload = Payload(COMPRESSIBLE, {"encoding": "binary/plain"})
        (encoded,) = await codec.encode([payload])
        assert encoded.encoding == ENCODING_COMPRESSED
        assert len(encoded.data) < len(payload.data)

    async def test_payload_below_threshold_is_left_alone(self) -> None:
        codec = CompressionCodec(threshold=100)
        payload = Payload(TINY, {"encoding": "binary/plain"})
        (encoded,) = await codec.encode([payload])
        assert encoded == payload

    async def test_incompressible_payload_above_threshold_is_left_alone(self) -> None:
        import os

        codec = CompressionCodec(threshold=10)
        # Random bytes are already near-maximum entropy: zlib will not shrink them.
        payload = Payload(os.urandom(2000), {"encoding": "binary/plain"})
        (encoded,) = await codec.encode([payload])
        assert encoded == payload
        assert encoded.encoding != ENCODING_COMPRESSED

    async def test_roundtrip_restores_original_bytes_and_encoding(self) -> None:
        codec = CompressionCodec(threshold=10)
        payload = Payload(COMPRESSIBLE, {"encoding": "json/plain", "extra": "kept"})
        (encoded,) = await codec.encode([payload])
        (decoded,) = await codec.decode([encoded])
        assert decoded == payload

    async def test_foreign_payload_passes_through_decode_unchanged(self) -> None:
        codec = CompressionCodec()
        foreign = Payload(b"abc", {"encoding": "json/plain"})
        (decoded,) = await codec.decode([foreign])
        assert decoded == foreign

    async def test_level_is_configurable(self) -> None:
        low = CompressionCodec(threshold=10, level=1)
        high = CompressionCodec(threshold=10, level=9)
        payload = Payload(COMPRESSIBLE, {"encoding": "binary/plain"})
        (low_encoded,) = await low.encode([payload])
        (high_encoded,) = await high.encode([payload])
        # Both must still round-trip regardless of the size trade-off between levels.
        assert (await low.decode([low_encoded]))[0] == payload
        assert (await high.decode([high_encoded]))[0] == payload


class TestAlgorithmRegistry:
    def test_zlib_is_registered_by_default(self) -> None:
        assert get_algorithm("zlib").name == "zlib"

    def test_unknown_algorithm_raises_codec_error(self) -> None:
        with pytest.raises(CodecError, match="unknown compression algorithm"):
            get_algorithm("does-not-exist")

    async def test_custom_algorithm_can_be_registered_and_used(self) -> None:
        class DoubleZlib:
            name = "double-zlib"

            def compress(self, data: bytes, level: int) -> bytes:
                return zlib.compress(zlib.compress(data, level), level)

            def decompress(self, data: bytes) -> bytes:
                return zlib.decompress(zlib.decompress(data))

        algorithm: CompressionAlgorithm = DoubleZlib()
        register_algorithm(algorithm)
        try:
            codec = CompressionCodec(algorithm=algorithm, threshold=10)
            payload = Payload(COMPRESSIBLE, {"encoding": "binary/plain"})
            (encoded,) = await codec.encode([payload])
            assert encoded.encoding == ENCODING_COMPRESSED
            (decoded,) = await codec.decode([encoded])
            assert decoded == payload
        finally:
            register_algorithm(ZlibAlgorithm())  # avoid leaking state across tests

    async def test_decoding_records_the_algorithm_used_even_if_default_changes(self) -> None:
        """A payload compressed with algorithm A must decode correctly even if the
        codec instance doing the decoding now defaults to algorithm B."""

        class Reverser:
            name = "reverser"

            def compress(self, data: bytes, level: int) -> bytes:
                return data[::-1]

            def decompress(self, data: bytes) -> bytes:
                return data[::-1]

        register_algorithm(Reverser())
        try:
            writer = CompressionCodec(algorithm=Reverser(), threshold=1)
            payload = Payload(b"abcdefgh", {"encoding": "binary/plain"})
            (encoded,) = await writer.encode([payload])

            reader = CompressionCodec(algorithm=ZlibAlgorithm(), threshold=1)
            (decoded,) = await reader.decode([encoded])
            assert decoded == payload
        finally:
            register_algorithm(ZlibAlgorithm())


@given(st.binary(min_size=0, max_size=2000))
async def test_roundtrip_property(data: bytes) -> None:
    codec = CompressionCodec(threshold=50)
    payload = Payload(data, {"encoding": "binary/plain"})
    (encoded,) = await codec.encode([payload])
    (decoded,) = await codec.decode([encoded])
    assert decoded == payload
