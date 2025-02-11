"""Tests for the :class:`PayloadCodec` protocol, marker helpers and :class:`CodecChain`."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest
from hypothesis import given
from hypothesis import strategies as st

from loomwork.codec.base import CodecChain, PayloadCodec, produced_by, unwrap, wrap
from loomwork.codec.payload import ENCODING_KEY, Payload


@dataclass(slots=True)
class UppercaseCodec:
    """A trivial marker codec used to exercise the base machinery in isolation."""

    marker: str = "test/uppercase"
    inner_key: str = "codec.uppercase.inner-encoding"
    calls: list[str] = field(default_factory=list)

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.calls.append("encode")
        return [
            wrap(
                payload,
                payload.data.upper(),
                produced_encoding=self.marker,
                inner_encoding_key=self.inner_key,
            )
            for payload in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.calls.append("decode")
        out = []
        for payload in payloads:
            if not produced_by(payload, self.marker):
                out.append(payload)
                continue
            out.append(unwrap(payload, payload.data.lower(), inner_encoding_key=self.inner_key))
        return out


@dataclass(slots=True)
class ReverseCodec:
    """Another trivial marker codec, used to test layering order."""

    marker: str = "test/reverse"
    inner_key: str = "codec.reverse.inner-encoding"

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            wrap(
                p,
                p.data[::-1],
                produced_encoding=self.marker,
                inner_encoding_key=self.inner_key,
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            unwrap(p, p.data[::-1], inner_encoding_key=self.inner_key)
            if produced_by(p, self.marker)
            else p
            for p in payloads
        ]


class TestProtocol:
    def test_codecs_satisfy_the_protocol_structurally(self) -> None:
        assert isinstance(UppercaseCodec(), PayloadCodec)
        assert isinstance(ReverseCodec(), PayloadCodec)
        assert isinstance(CodecChain([]), PayloadCodec)

    def test_non_codec_does_not_satisfy_protocol(self) -> None:
        assert not isinstance(object(), PayloadCodec)


class TestWrapUnwrap:
    async def test_wrap_stashes_prior_encoding_and_sets_marker(self) -> None:
        original = Payload(b"hello", {"encoding": "json/plain", "custom": "1"})
        wrapped = wrap(original, b"HELLO", produced_encoding="binary/x", inner_encoding_key="inner")
        assert wrapped.encoding == "binary/x"
        assert wrapped.metadata["inner"] == "json/plain"
        assert wrapped.metadata["custom"] == "1"
        assert wrapped.data == b"HELLO"

    async def test_unwrap_restores_prior_encoding_and_drops_marker_keys(self) -> None:
        original = Payload(b"hello", {"encoding": "json/plain"})
        wrapped = wrap(original, b"HELLO", produced_encoding="binary/x", inner_encoding_key="inner")
        restored = unwrap(wrapped, b"hello", inner_encoding_key="inner")
        assert restored == original

    async def test_wrap_does_not_mutate_original_metadata(self) -> None:
        original = Payload(b"hello", {"encoding": "json/plain"})
        wrap(original, b"HELLO", produced_encoding="binary/x", inner_encoding_key="inner")
        assert original.metadata == {"encoding": "json/plain"}

    def test_produced_by(self) -> None:
        assert produced_by(Payload(b"", {ENCODING_KEY: "a"}), "a")
        assert not produced_by(Payload(b"", {ENCODING_KEY: "a"}), "b")
        assert not produced_by(Payload(b""), "a")


class TestCodecChain:
    async def test_empty_chain_is_identity(self) -> None:
        chain = CodecChain([])
        payloads = [Payload(b"x", {"encoding": "json/plain"})]
        assert await chain.encode(payloads) == payloads
        assert await chain.decode(payloads) == payloads

    async def test_single_codec_roundtrip(self) -> None:
        chain = CodecChain([UppercaseCodec()])
        payload = Payload(b"hello", {"encoding": "json/plain"})
        (encoded,) = await chain.encode([payload])
        assert encoded.data == b"HELLO"
        (decoded,) = await chain.decode([encoded])
        assert decoded == payload

    async def test_codecs_apply_in_order_on_encode_and_reverse_on_decode(self) -> None:
        # upper(reverse(x)) on encode; decode must undo reverse first, then upper.
        chain = CodecChain([ReverseCodec(), UppercaseCodec()])
        payload = Payload(b"ab", {"encoding": "json/plain"})
        (encoded,) = await chain.encode([payload])
        assert encoded.data == b"BA"
        assert encoded.encoding == "test/uppercase"
        (decoded,) = await chain.decode([encoded])
        assert decoded == payload

    async def test_decode_is_order_sensitive(self) -> None:
        """Decoding in encode order (instead of reverse) would corrupt the payload."""
        upper, reverse = UppercaseCodec(), ReverseCodec()
        chain = CodecChain([reverse, upper])
        payload = Payload(b"ab", {"encoding": "json/plain"})
        (encoded,) = await chain.encode([payload])

        wrong_order = CodecChain([upper, reverse])  # decode() reverses *this* list
        (decoded,) = await wrong_order.decode([encoded])
        assert decoded != payload

    async def test_foreign_payload_passes_through_every_codec_unchanged(self) -> None:
        chain = CodecChain([UppercaseCodec(), ReverseCodec()])
        foreign = Payload(b"untouched", {"encoding": "json/plain"})
        (decoded,) = await chain.decode([foreign])
        assert decoded == foreign

    async def test_enabling_a_new_codec_keeps_old_payloads_decodable(self) -> None:
        """Simulates turning on a new codec after payloads already exist without it."""
        old_payload = Payload(b'{"a": 1}', {"encoding": "json/plain"})
        chain_without_codec = CodecChain([])
        (stored,) = await chain_without_codec.encode([old_payload])

        chain_with_new_codec = CodecChain([UppercaseCodec()])
        (decoded,) = await chain_with_new_codec.decode([stored])
        assert decoded == old_payload

    async def test_batch_preserves_order(self) -> None:
        chain = CodecChain([UppercaseCodec()])
        payloads = [Payload(f"v{i}".encode(), {"encoding": "json/plain"}) for i in range(20)]
        encoded = await chain.encode(payloads)
        decoded = await chain.decode(encoded)
        assert decoded == payloads

    @given(st.binary(max_size=200))
    async def test_wrap_unwrap_roundtrip_property(self, data: bytes) -> None:
        # ReverseCodec (unlike UppercaseCodec) is a true bijection on bytes, so it is
        # the right toy codec for checking the wrap/unwrap machinery round-trips for
        # arbitrary data.
        codec = ReverseCodec()
        payload = Payload(data, {"encoding": "binary/plain"})
        (encoded,) = await codec.encode([payload])
        (decoded,) = await codec.decode([encoded])
        assert decoded == payload


@pytest.mark.asyncio
async def test_chain_is_safe_under_concurrent_use() -> None:
    import asyncio

    chain = CodecChain([UppercaseCodec(), ReverseCodec()])
    payloads = [Payload(f"item-{i}".encode(), {"encoding": "json/plain"}) for i in range(50)]

    async def roundtrip(payload: Payload) -> Payload:
        (encoded,) = await chain.encode([payload])
        (decoded,) = await chain.decode([encoded])
        return decoded

    results = await asyncio.gather(*(roundtrip(p) for p in payloads))
    assert results == payloads
