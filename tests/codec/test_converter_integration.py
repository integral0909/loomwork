"""Tests for :class:`DataConverter`'s codec integration.

These exercise the ``_async`` methods added for the codec chain and confirm the
pre-existing synchronous methods are completely unaffected by attaching a codec (other
code -- workflow replay in particular -- depends on the synchronous methods never doing
I/O or changing behaviour).
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from loomwork.codec.base import CodecChain
from loomwork.codec.compression import CompressionCodec
from loomwork.codec.converter import DataConverter
from loomwork.codec.encryption import EncryptionCodec
from loomwork.codec.keyring import StaticKeyRing
from loomwork.codec.limits import PayloadSizeLimitCodec
from loomwork.codec.payload import ENCODING_JSON, Payload

KEY = b"k" * 32


def make_chain() -> CodecChain:
    return CodecChain(
        [
            CompressionCodec(threshold=10),
            EncryptionCodec(StaticKeyRing({"k1": KEY}, current="k1")),
            PayloadSizeLimitCodec(max_bytes=1_000_000),
        ]
    )


class TestAsyncSingleValue:
    async def test_encode_async_without_codec_matches_sync_encode(self) -> None:
        converter = DataConverter()
        value = {"a": 1, "b": [1, 2, 3]}
        assert await converter.encode_async(value) == converter.encode(value)

    async def test_encode_decode_async_roundtrip_with_codec(self) -> None:
        converter = DataConverter(codec=make_chain())
        value = {"order_id": "o1", "total": 4200, "note": "x" * 200}
        payload = await converter.encode_async(value)
        assert payload.encoding not in (ENCODING_JSON, "")
        assert await converter.decode_async(payload) == value

    async def test_decode_async_passes_through_payload_written_without_a_codec(self) -> None:
        """A codec added after payloads already exist must still decode old ones."""
        plain_converter = DataConverter()
        old_payload = plain_converter.encode({"legacy": True})

        upgraded_converter = DataConverter(codec=make_chain())
        assert await upgraded_converter.decode_async(old_payload) == {"legacy": True}


class TestAsyncArgsAndResult:
    async def test_encode_args_async_without_codec_matches_sync(self) -> None:
        converter = DataConverter()
        args = [1, "two", {"three": 3}]
        assert await converter.encode_args_async(args) == converter.encode_args(args)

    async def test_encode_decode_args_async_roundtrip_with_codec(self) -> None:
        converter = DataConverter(codec=make_chain())

        async def fn(order_id: str, total_cents: int, note: str) -> None: ...

        args = ["o1", 4200, "y" * 500]
        payloads = await converter.encode_args_async(args)
        assert all(p.encoding != ENCODING_JSON for p in payloads)  # all went through codec
        decoded = await converter.decode_args_async(fn, payloads)
        assert decoded == args

    async def test_decode_result_async_roundtrip_with_codec(self) -> None:
        converter = DataConverter(codec=make_chain())

        async def fn() -> dict[str, int]:
            raise NotImplementedError

        payload = await converter.encode_async({"total": 1})
        assert await converter.decode_result_async(fn, payload) == {"total": 1}

    async def test_empty_args_list(self) -> None:
        converter = DataConverter(codec=make_chain())
        assert await converter.encode_args_async([]) == []
        assert await converter.decode_args_async(lambda: None, []) == []


class TestSyncMethodsUnaffectedByCodec:
    """Regression guard: attaching a codec must not change synchronous behaviour."""

    def test_sync_encode_ignores_codec(self) -> None:
        converter = DataConverter(codec=make_chain())
        value = {"a": 1}
        payload = converter.encode(value)
        assert payload.encoding == ENCODING_JSON  # codec never touched it

    def test_sync_decode_ignores_codec(self) -> None:
        converter = DataConverter(codec=make_chain())
        payload = Payload(b'{"a": 1}', {"encoding": ENCODING_JSON})
        assert converter.decode(payload) == {"a": 1}

    def test_sync_encode_args_and_decode_args_ignore_codec(self) -> None:
        converter = DataConverter(codec=make_chain())

        def fn(x: int) -> None: ...

        payloads = converter.encode_args([7])
        assert payloads[0].encoding == ENCODING_JSON
        assert converter.decode_args(fn, payloads) == [7]

    def test_default_codec_is_none(self) -> None:
        assert DataConverter().codec is None

    def test_plain_data_converter_behaviour_is_identical_with_and_without_codec_attached(
        self,
    ) -> None:
        plain = DataConverter()
        with_codec = DataConverter(codec=make_chain())
        for value in [None, 1, "x", [1, 2], {"a": {"b": 1}}, b"\x00\x01"]:
            assert plain.encode(value) == with_codec.encode(value)


class RecordingCodec:
    """Counts calls so we can assert the async path actually invokes the codec."""

    def __init__(self) -> None:
        self.encode_calls = 0
        self.decode_calls = 0

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.encode_calls += 1
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.decode_calls += 1
        return list(payloads)


class TestCodecIsActuallyInvoked:
    async def test_encode_async_invokes_codec_once_per_call(self) -> None:
        codec = RecordingCodec()
        converter = DataConverter(codec=codec)
        await converter.encode_async(1)
        await converter.encode_args_async([1, 2, 3])
        assert codec.encode_calls == 2

    async def test_decode_async_invokes_codec_once_per_call(self) -> None:
        codec = RecordingCodec()
        converter = DataConverter(codec=codec)
        payload = converter.encode(1)
        await converter.decode_async(payload)
        await converter.decode_args_async(lambda x: None, [payload])
        assert codec.decode_calls == 2

    async def test_sync_methods_never_invoke_codec(self) -> None:
        codec = RecordingCodec()
        converter = DataConverter(codec=codec)
        payload = converter.encode(1)
        converter.decode(payload)
        converter.encode_args([1])
        converter.decode_args(lambda x: None, [payload])
        assert codec.encode_calls == 0
        assert codec.decode_calls == 0


@pytest.mark.parametrize(
    "value",
    [None, 0, "text", True, [1, "a", None], {"a": {"b": [1, 2]}}, b"\x00\xff"],
)
async def test_various_value_types_survive_the_full_chain(value: object) -> None:
    converter = DataConverter(codec=make_chain())
    payload = await converter.encode_async(value)
    assert await converter.decode_async(payload) == value
