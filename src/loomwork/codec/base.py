"""The :class:`PayloadCodec` protocol and codec composition.

A codec transforms already-encoded payloads (bytes produced by a :class:`.converter.
DataConverter`) before they reach the store, and reverses the transformation on the way
back. Codecs are async because decoding may need to call out to a key management
service.

Every codec marks the payloads it produced by giving them a distinctive
:data:`~loomwork.codec.payload.ENCODING_KEY` value (for example ``binary/encrypted``)
and stashing whatever the encoding was *before* the codec ran under a metadata key that
belongs to that codec alone (see :func:`wrap` / :func:`unwrap`). Decoding checks that
marker: a payload the codec did not produce is returned unchanged. This is what lets a
:class:`CodecChain` skip payloads that were written before a codec existed (turning on
encryption on an existing deployment does not break replay of runs recorded before that
change) and what lets several codecs be layered in any order without one stepping on
another's bookkeeping, as long as each uses its own ``inner_encoding_key``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from loomwork.codec.payload import ENCODING_KEY, Payload

_NO_EXTRA: Mapping[str, str] = {}
_NO_DROP: tuple[str, ...] = ()


@runtime_checkable
class PayloadCodec(Protocol):
    """Encodes and decodes batches of payloads.

    Implementations must be safe to call concurrently: the engine may encode payloads
    for many runs at once. Batches exist so a codec that talks to a remote key service
    can amortize the round trip across an argument list instead of paying it per value.
    """

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Transform ``payloads`` on their way to storage."""
        ...

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Reverse :meth:`encode`. Payloads this codec did not produce pass through."""
        ...


def produced_by(payload: Payload, produced_encoding: str) -> bool:
    """Whether ``payload`` carries the marker of a codec that wrapped it."""
    return payload.encoding == produced_encoding


def wrap(
    payload: Payload,
    data: bytes,
    *,
    produced_encoding: str,
    inner_encoding_key: str,
    extra_metadata: Mapping[str, str] = _NO_EXTRA,
) -> Payload:
    """Build the payload a codec produces when it transforms ``payload``.

    Stashes ``payload``'s current encoding under ``inner_encoding_key`` and sets the
    top-level encoding to ``produced_encoding``, so :func:`unwrap` can reverse it later
    and :func:`produced_by` can recognise it.
    """
    metadata = {
        **payload.metadata,
        **extra_metadata,
        inner_encoding_key: payload.encoding,
        ENCODING_KEY: produced_encoding,
    }
    return Payload(data, metadata)


def unwrap(
    payload: Payload,
    data: bytes,
    *,
    inner_encoding_key: str,
    drop_keys: Iterable[str] = _NO_DROP,
) -> Payload:
    """Reverse :func:`wrap`. Only call this once :func:`produced_by` has confirmed the
    payload is this codec's to unwrap."""
    metadata = {
        k: v for k, v in payload.metadata.items() if k != inner_encoding_key and k not in drop_keys
    }
    metadata[ENCODING_KEY] = payload.metadata.get(inner_encoding_key, "")
    return Payload(data, metadata)


@dataclass(slots=True)
class CodecChain:
    """Applies codecs in order on encode and in reverse order on decode.

    A ``CodecChain`` is itself a :class:`PayloadCodec`, so chains can be nested if that
    ever becomes useful.
    """

    codecs: Sequence[PayloadCodec]

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        current: Sequence[Payload] = payloads
        for codec in self.codecs:
            current = await codec.encode(current)
        return list(current)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        current: Sequence[Payload] = payloads
        for codec in reversed(self.codecs):
            current = await codec.decode(current)
        return list(current)
