"""In-memory blob store, for tests and single-process embedded use."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime

from loomwork.blobs.base import BlobInfo
from loomwork.blobs.errors import BlobNotFoundError


class MemoryBlobStore:
    """Holds blobs in a process-local dict. Nothing survives a restart."""

    def __init__(self) -> None:
        self._blobs: dict[str, tuple[bytes, dict[str, str], datetime]] = {}
        self._lock = asyncio.Lock()

    async def put(self, key: str, data: bytes, metadata: Mapping[str, str] | None = None) -> None:
        async with self._lock:
            self._blobs[key] = (bytes(data), dict(metadata or {}), datetime.now(UTC))

    async def get(self, key: str) -> bytes:
        async with self._lock:
            try:
                data, _, _ = self._blobs[key]
            except KeyError:
                raise BlobNotFoundError(key) from None
            return data

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._blobs.pop(key, None)

    async def exists(self, key: str) -> bool:
        async with self._lock:
            return key in self._blobs

    async def list(self, prefix: str = "") -> AsyncIterator[BlobInfo]:
        async with self._lock:
            snapshot = [
                (key, len(data), metadata, written_at)
                for key, (data, metadata, written_at) in self._blobs.items()
                if key.startswith(prefix)
            ]
        for key, size, metadata, written_at in snapshot:
            yield BlobInfo(key=key, size=size, last_modified=written_at, metadata=metadata)
