"""The "claim check" codec: moves large payloads into a blob store.

Workflow histories are meant to stay small -- they are replayed in full, kept around for
the life of a run, and often archived. A step that returns a multi-megabyte report or
dataset would otherwise put that entire value in every tick's history. :class:`OffloadCodec`
intercepts payloads above ``threshold_bytes`` on encode, writes them to a
:class:`~loomwork.blobs.base.BlobStore` under a key derived from their SHA-256 hash, and
replaces them with a small reference payload (the hash, the key, the size); decode fetches
the blob back and verifies the hash before handing the bytes onward. Everything below the
threshold passes through untouched, at the cost of one length check.

**Ordering.** ``OffloadCodec`` must be the *outermost* codec: last in the list given to a
:class:`~loomwork.codec.base.CodecChain` on encode (so it runs after compression and
encryption), first on decode (reversed order -- see :class:`CodecChain`). Concretely::

    CodecChain([CompressionCodec(), EncryptionCodec(keyring), OffloadCodec(store),
                PayloadSizeLimitCodec(max_bytes=...)])

This means the blob store only ever holds ciphertext (when encryption is configured) --
it never sees plaintext, exactly like the primary store. It also means
:class:`~loomwork.codec.limits.PayloadSizeLimitCodec` (if used) sees the small reference
payload rather than the pre-offload data, so it should run *after* ``OffloadCodec``: the
size limit is then a limit on what reaches the store's history, and a value that would
have exceeded it is instead offloaded and replaced by a few dozen bytes.

**Integrity.** The blob key is content-addressed (``sha256(payload bytes)``, optionally
namespaced by ``key_prefix``), so two payloads with identical bytes -- the common case for
retried ticks re-encoding the same result -- always dedupe onto the same blob and a
concurrent ``put`` of that key is always writing identical content, making the store's own
idempotency and atomicity the only property required from it: no coordination between
writers is needed. The hash is verified again on every decode, so a blob whose bytes were
altered after it was written (bit rot, a corrupted backup, tampering, if store integrity
protections were bypassed) raises :class:`~loomwork.codec.errors.IntegrityError` instead of
silently returning wrong data.

**Missing blobs.** If a referenced blob is gone (deleted by a misconfigured retention
policy, garbage collection that ran too aggressively -- see ``loomwork.blobs.gc``, or
operator error), decode raises :class:`CodecError` naming the key, rather than a bare
:class:`~loomwork.blobs.errors.BlobNotFoundError` with no payload context.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass

from loomwork.blobs.base import BlobStore
from loomwork.blobs.errors import BlobNotFoundError
from loomwork.codec.base import produced_by, unwrap, wrap
from loomwork.codec.errors import CodecError, IntegrityError
from loomwork.codec.payload import Payload

ENCODING_OFFLOADED = "binary/offloaded"

#: Payloads at or above this size move to the blob store by default.
DEFAULT_THRESHOLD_BYTES = 128 * 1024

_INNER_ENCODING_KEY = "codec.offload.inner-encoding"
_KEY_META = "codec.offload.key"
_SIZE_META = "codec.offload.size"
_HASH_META = "codec.offload.sha256"


def offload_key(payload: Payload) -> str | None:
    """The blob store key ``payload`` references, or ``None`` if it is not an offload
    reference.

    Used by ``loomwork.blobs.gc`` to find which blobs a history still needs without
    depending on anything else in this module.
    """
    if not produced_by(payload, ENCODING_OFFLOADED):
        return None
    return payload.metadata.get(_KEY_META)


@dataclass(slots=True)
class OffloadCodec:
    """Moves payloads of ``threshold_bytes`` or more into ``store``.

    Attributes:
        store: where offloaded bytes are written and read back from.
        threshold_bytes: payloads smaller than this are left alone.
        key_prefix: prepended to every blob key (for example ``"loomwork/payloads/"``),
            useful for sharing a bucket or root directory with other data, or for
            namespacing GC scans (see ``loomwork.blobs.gc``'s ``prefix`` argument).
    """

    store: BlobStore
    threshold_bytes: int = DEFAULT_THRESHOLD_BYTES
    key_prefix: str = ""

    def __post_init__(self) -> None:
        if self.threshold_bytes <= 0:
            raise ValueError("threshold_bytes must be positive")

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [await self._encode_one(p) for p in payloads]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [await self._decode_one(p) for p in payloads]

    async def _encode_one(self, payload: Payload) -> Payload:
        if len(payload.data) < self.threshold_bytes:
            return payload
        digest = hashlib.sha256(payload.data).hexdigest()
        key = f"{self.key_prefix}{digest}"
        # Content-addressed: an identical payload already offloaded writes the same
        # key with the same bytes, so re-putting it is redundant but never wrong. The
        # existence check is purely an optimisation to skip that redundant write; it
        # introduces no race, because whichever of two concurrent first-time writers
        # of this key finishes last still writes the same bytes as the other.
        if not await self.store.exists(key):
            await self.store.put(
                key,
                payload.data,
                metadata={_HASH_META: digest, _SIZE_META: str(len(payload.data))},
            )
        return wrap(
            payload,
            b"",
            produced_encoding=ENCODING_OFFLOADED,
            inner_encoding_key=_INNER_ENCODING_KEY,
            extra_metadata={
                _KEY_META: key,
                _SIZE_META: str(len(payload.data)),
                _HASH_META: digest,
            },
        )

    async def _decode_one(self, payload: Payload) -> Payload:
        if not produced_by(payload, ENCODING_OFFLOADED):
            return payload
        try:
            key = payload.metadata[_KEY_META]
            expected_hash = payload.metadata[_HASH_META]
        except KeyError as exc:
            raise IntegrityError(f"offloaded payload is missing {exc} metadata") from None
        try:
            data = await self.store.get(key)
        except BlobNotFoundError as exc:
            raise CodecError(
                f"blob {key!r} referenced by an offloaded payload was not found in the "
                "blob store (it may have been deleted by a retention policy or garbage "
                "collection that ran before every reference to it was gone)"
            ) from exc
        actual_hash = hashlib.sha256(data).hexdigest()
        if actual_hash != expected_hash:
            raise IntegrityError(
                f"blob {key!r} does not match its recorded hash (expected {expected_hash}, "
                f"got {actual_hash}): the blob store returned corrupted or substituted data"
            )
        return unwrap(
            payload,
            data,
            inner_encoding_key=_INNER_ENCODING_KEY,
            drop_keys=(_KEY_META, _SIZE_META, _HASH_META),
        )
