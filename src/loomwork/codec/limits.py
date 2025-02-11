"""Payload size limits.

The store is not designed to hold arbitrarily large blobs, and an unbounded payload from
a misbehaving workflow can degrade it for everyone. :class:`PayloadSizeLimitCodec`
rejects payloads above a configured size with a clear error, before they ever reach a
store call. Put it last in a :class:`~loomwork.codec.base.CodecChain` so the limit
applies to what actually gets stored (after compression and encryption), not the
original, possibly larger, plaintext.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from loomwork.codec.errors import PayloadTooLargeError
from loomwork.codec.payload import Payload

DEFAULT_MAX_BYTES = 2 * 1024 * 1024  # 2 MiB


@dataclass(slots=True)
class PayloadSizeLimitCodec:
    """Rejects payloads larger than ``max_bytes`` on encode.

    This codec never transforms bytes, so decoding is always a pass-through: a payload
    that was written before the limit existed, or under a looser one, still has to be
    readable.
    """

    max_bytes: int = DEFAULT_MAX_BYTES

    def __post_init__(self) -> None:
        if self.max_bytes <= 0:
            raise ValueError("max_bytes must be positive")

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        for payload in payloads:
            size = len(payload.data)
            if size > self.max_bytes:
                raise PayloadTooLargeError(size, self.max_bytes)
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(payloads)
