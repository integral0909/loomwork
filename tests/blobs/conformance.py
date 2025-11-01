"""Behavioural contract every :class:`~loomwork.blobs.base.BlobStore` must satisfy.

Mirrors ``tests/store_conformance.py``'s pattern: one mixin class, run against every
backend (memory, filesystem, and a fake S3 behind ``httpx.MockTransport``) so a bug in
one implementation can't hide behind tests only the others happen to exercise.
"""

from __future__ import annotations

import asyncio

import pytest

from loomwork.blobs.base import ListableBlobStore
from loomwork.blobs.errors import BlobNotFoundError


class BlobStoreConformance:
    """Mix-in test class; subclasses provide the ``store`` fixture."""

    async def test_put_then_get_round_trips_bytes(self, store: ListableBlobStore) -> None:
        await store.put("k1", b"hello world")
        assert await store.get("k1") == b"hello world"

    async def test_put_empty_bytes(self, store: ListableBlobStore) -> None:
        await store.put("empty", b"")
        assert await store.get("empty") == b""

    async def test_get_missing_key_raises(self, store: ListableBlobStore) -> None:
        with pytest.raises(BlobNotFoundError) as exc_info:
            await store.get("does-not-exist")
        assert "does-not-exist" in str(exc_info.value)

    async def test_exists(self, store: ListableBlobStore) -> None:
        assert not await store.exists("k2")
        await store.put("k2", b"data")
        assert await store.exists("k2")

    async def test_put_overwrites_existing_key(self, store: ListableBlobStore) -> None:
        await store.put("k3", b"first")
        await store.put("k3", b"second")
        assert await store.get("k3") == b"second"

    async def test_delete_removes_the_blob(self, store: ListableBlobStore) -> None:
        await store.put("k4", b"data")
        await store.delete("k4")
        assert not await store.exists("k4")
        with pytest.raises(BlobNotFoundError):
            await store.get("k4")

    async def test_delete_missing_key_is_not_an_error(self, store: ListableBlobStore) -> None:
        await store.delete("never-existed")  # must not raise

    async def test_put_is_idempotent(self, store: ListableBlobStore) -> None:
        await store.put("k5", b"same bytes", metadata={"a": "1"})
        await store.put("k5", b"same bytes", metadata={"a": "1"})
        assert await store.get("k5") == b"same bytes"

    async def test_concurrent_put_of_the_same_key_is_safe(self, store: ListableBlobStore) -> None:
        # The offload codec only ever concurrently puts the same key with identical
        # bytes (it is content-addressed); a store just needs each individual put to be
        # atomic, not to coordinate between writers.
        await asyncio.gather(*(store.put("concurrent", b"identical payload") for _ in range(8)))
        assert await store.get("concurrent") == b"identical payload"

    async def test_list_returns_every_key_with_correct_size(self, store: ListableBlobStore) -> None:
        await store.put("list/a", b"12345")
        await store.put("list/b", b"1234567890")
        found = {info.key: info.size async for info in store.list()}
        assert found["list/a"] == 5
        assert found["list/b"] == 10

    async def test_list_filters_by_prefix(self, store: ListableBlobStore) -> None:
        await store.put("prefix-a/1", b"x")
        await store.put("prefix-a/2", b"y")
        await store.put("prefix-b/1", b"z")
        found = {info.key async for info in store.list("prefix-a/")}
        assert found == {"prefix-a/1", "prefix-a/2"}

    async def test_list_reflects_deletes(self, store: ListableBlobStore) -> None:
        await store.put("deleted-key", b"x")
        await store.delete("deleted-key")
        found = {info.key async for info in store.list()}
        assert "deleted-key" not in found
