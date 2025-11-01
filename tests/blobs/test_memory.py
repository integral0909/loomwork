from __future__ import annotations

import pytest

from loomwork.blobs.memory import MemoryBlobStore
from tests.blobs.conformance import BlobStoreConformance


@pytest.fixture
def store() -> MemoryBlobStore:
    return MemoryBlobStore()


class TestMemoryBlobStore(BlobStoreConformance):
    pass


async def test_list_reports_metadata() -> None:
    store = MemoryBlobStore()
    await store.put("with-metadata", b"x", metadata={"sha256": "abc", "size": "1"})
    infos = [info async for info in store.list("with-metadata")]
    assert len(infos) == 1
    assert dict(infos[0].metadata) == {"sha256": "abc", "size": "1"}


async def test_two_instances_are_independent() -> None:
    a, b = MemoryBlobStore(), MemoryBlobStore()
    await a.put("k", b"data")
    assert not await b.exists("k")
