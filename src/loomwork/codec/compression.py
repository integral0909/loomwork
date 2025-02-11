"""Compression codec.

Compresses payload bytes with zlib (stdlib, no extra dependency) below a size threshold
skipped so we don't spend CPU shrinking values too small to benefit, and with a
worthwhile-ratio check so a payload that doesn't actually get smaller (already-compressed
media, high-entropy data) is stored as-is rather than carrying compression overhead for
nothing.
"""

from __future__ import annotations

import zlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from loomwork.codec.base import produced_by, unwrap, wrap
from loomwork.codec.errors import CodecError
from loomwork.codec.payload import Payload

ENCODING_COMPRESSED = "binary/compressed"

_INNER_ENCODING_KEY = "codec.compression.inner-encoding"
_ALGORITHM_KEY = "codec.compression.algorithm"

DEFAULT_THRESHOLD = 512
DEFAULT_LEVEL = 6


class CompressionAlgorithm(Protocol):
    """A pluggable compression backend.

    Register additional algorithms (zstd, brotli, ...) with :func:`register_algorithm`
    without touching :class:`CompressionCodec`.
    """

    name: str

    def compress(self, data: bytes, level: int) -> bytes: ...
    def decompress(self, data: bytes) -> bytes: ...


@dataclass(slots=True)
class ZlibAlgorithm:
    """The default, stdlib-only compression algorithm."""

    name: str = "zlib"

    def compress(self, data: bytes, level: int) -> bytes:
        return zlib.compress(data, level)

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


_ALGORITHMS: dict[str, CompressionAlgorithm] = {}


def register_algorithm(algorithm: CompressionAlgorithm) -> None:
    """Make ``algorithm`` available for :class:`CompressionCodec` and for decoding
    payloads compressed with it."""
    _ALGORITHMS[algorithm.name] = algorithm


def get_algorithm(name: str) -> CompressionAlgorithm:
    try:
        return _ALGORITHMS[name]
    except KeyError:
        raise CodecError(f"unknown compression algorithm {name!r}") from None


register_algorithm(ZlibAlgorithm())


@dataclass(slots=True)
class CompressionCodec:
    """Compresses payloads above ``threshold`` bytes when it actually shrinks them.

    Attributes:
        algorithm: the compression backend. Defaults to zlib.
        threshold: payloads smaller than this many bytes are left uncompressed; the
            fixed per-payload overhead of most compressors makes it a net loss below a
            few hundred bytes.
        level: passed through to the algorithm (zlib: 0-9, higher is slower/smaller).
    """

    algorithm: CompressionAlgorithm = field(default_factory=ZlibAlgorithm)
    threshold: int = DEFAULT_THRESHOLD
    level: int = DEFAULT_LEVEL

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [self._encode_one(p) for p in payloads]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [self._decode_one(p) for p in payloads]

    def _encode_one(self, payload: Payload) -> Payload:
        if len(payload.data) < self.threshold:
            return payload
        compressed = self.algorithm.compress(payload.data, self.level)
        if len(compressed) >= len(payload.data):
            return payload
        return wrap(
            payload,
            compressed,
            produced_encoding=ENCODING_COMPRESSED,
            inner_encoding_key=_INNER_ENCODING_KEY,
            extra_metadata={_ALGORITHM_KEY: self.algorithm.name},
        )

    def _decode_one(self, payload: Payload) -> Payload:
        if not produced_by(payload, ENCODING_COMPRESSED):
            return payload
        algorithm_name = payload.metadata.get(_ALGORITHM_KEY, "")
        algorithm = get_algorithm(algorithm_name) if algorithm_name else self.algorithm
        decompressed = algorithm.decompress(payload.data)
        return unwrap(
            payload,
            decompressed,
            inner_encoding_key=_INNER_ENCODING_KEY,
            drop_keys=(_ALGORITHM_KEY,),
        )
