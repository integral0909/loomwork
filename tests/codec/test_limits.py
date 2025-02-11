"""Tests for :class:`PayloadSizeLimitCodec`."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from loomwork.codec.errors import PayloadTooLargeError
from loomwork.codec.limits import PayloadSizeLimitCodec
from loomwork.codec.payload import Payload


class TestSizeLimit:
    async def test_payload_within_limit_passes_through(self) -> None:
        codec = PayloadSizeLimitCodec(max_bytes=100)
        payload = Payload(b"x" * 50, {"encoding": "binary/plain"})
        assert await codec.encode([payload]) == [payload]

    async def test_payload_exactly_at_limit_passes(self) -> None:
        codec = PayloadSizeLimitCodec(max_bytes=10)
        payload = Payload(b"x" * 10, {"encoding": "binary/plain"})
        assert await codec.encode([payload]) == [payload]

    async def test_oversized_payload_raises_clear_error(self) -> None:
        codec = PayloadSizeLimitCodec(max_bytes=10)
        payload = Payload(b"x" * 11, {"encoding": "binary/plain"})
        with pytest.raises(PayloadTooLargeError) as exc_info:
            await codec.encode([payload])
        assert exc_info.value.size == 11
        assert exc_info.value.limit == 10
        assert "11" in str(exc_info.value)
        assert "10" in str(exc_info.value)

    async def test_batch_reports_the_first_offending_payload(self) -> None:
        codec = PayloadSizeLimitCodec(max_bytes=5)
        ok = Payload(b"ok", {"encoding": "binary/plain"})
        oversized = Payload(b"way too big", {"encoding": "binary/plain"})
        with pytest.raises(PayloadTooLargeError):
            await codec.encode([ok, oversized])

    async def test_decode_never_enforces_the_limit(self) -> None:
        """A payload stored before the limit existed, or under a looser one, must
        still be readable -- the limit only guards what gets written."""
        codec = PayloadSizeLimitCodec(max_bytes=5)
        big = Payload(b"x" * 1000, {"encoding": "binary/plain"})
        assert await codec.decode([big]) == [big]

    def test_non_positive_limit_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            PayloadSizeLimitCodec(max_bytes=0)
        with pytest.raises(ValueError, match="positive"):
            PayloadSizeLimitCodec(max_bytes=-1)

    def test_default_limit_is_reasonable(self) -> None:
        codec = PayloadSizeLimitCodec()
        assert codec.max_bytes > 0


@given(st.integers(min_value=1, max_value=64))
async def test_property_boundary_is_inclusive(limit: int) -> None:
    codec = PayloadSizeLimitCodec(max_bytes=limit)
    at_limit = Payload(b"x" * limit, {"encoding": "binary/plain"})
    over_limit = Payload(b"x" * (limit + 1), {"encoding": "binary/plain"})
    assert await codec.encode([at_limit]) == [at_limit]
    with pytest.raises(PayloadTooLargeError):
        await codec.encode([over_limit])
