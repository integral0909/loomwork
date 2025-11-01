"""The :class:`BlobStore` protocol: content storage for payloads too large to keep in a
workflow history.

A blob store is deliberately a much smaller contract than :class:`~loomwork.store.base.
Store`: it holds opaque bytes under a key, nothing more. ``loomwork.codec.offload``
builds "claim check" semantics -- content-addressed keys, integrity verification,
garbage collection -- on top of it; the store itself never interprets a key or its
bytes.

``list`` is split into its own :class:`ListableBlobStore` protocol because it is not
needed for the codec's hot path (only :meth:`BlobStore.put` and :meth:`BlobStore.get`
are) and a store backed by something that cannot enumerate its own keys efficiently
(some object storage gateways, for instance) can still implement plain ``BlobStore``.
Garbage collection (``loomwork.blobs.gc``) requires ``ListableBlobStore``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class BlobInfo:
    """One entry returned by :meth:`ListableBlobStore.list`."""

    key: str
    size: int
    last_modified: datetime
    metadata: Mapping[str, str] = field(default_factory=dict)


@runtime_checkable
class BlobStore(Protocol):
    """Content-addressable-agnostic storage for opaque byte blobs.

    Implementations must tolerate concurrent calls, including concurrent :meth:`put`
    calls for the *same* key (the offload codec's keys are content hashes, so two
    concurrent writers of the same key always carry identical bytes -- the store never
    needs to coordinate between them, only make each individual call atomic).
    """

    async def put(self, key: str, data: bytes, metadata: Mapping[str, str] | None = None) -> None:
        """Store ``data`` under ``key``, replacing anything already there.

        Idempotent: putting the same key twice (with the same bytes, which is all the
        offload codec ever does) leaves the store in the same observable state as
        putting it once.
        """
        ...

    async def get(self, key: str) -> bytes:
        """Return the bytes stored under ``key``.

        Raises:
            ~loomwork.blobs.errors.BlobNotFoundError: no blob exists under ``key``.
        """
        ...

    async def delete(self, key: str) -> None:
        """Remove ``key``. Not an error if it does not exist."""
        ...

    async def exists(self, key: str) -> bool:
        """Whether a blob is stored under ``key``."""
        ...


@runtime_checkable
class ListableBlobStore(BlobStore, Protocol):
    """A :class:`BlobStore` that can also enumerate its own keys, for garbage
    collection."""

    def list(self, prefix: str = "") -> AsyncIterator[BlobInfo]:
        """Stream every stored blob whose key starts with ``prefix``.

        An async generator rather than a list: a store holding millions of blobs must
        never be forced into memory at once (see ``loomwork.blobs.gc``, which is the
        only caller that needs this method).
        """
        ...
