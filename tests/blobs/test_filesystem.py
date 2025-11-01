from __future__ import annotations

from pathlib import Path

import pytest

from loomwork.blobs.filesystem import FileSystemBlobStore
from tests.blobs.conformance import BlobStoreConformance


@pytest.fixture
def store(tmp_path: Path) -> FileSystemBlobStore:
    return FileSystemBlobStore(tmp_path / "blobs")


class TestFileSystemBlobStore(BlobStoreConformance):
    pass


async def test_creates_root_directory(tmp_path: Path) -> None:
    root = tmp_path / "does" / "not" / "exist" / "yet"
    assert not root.exists()
    FileSystemBlobStore(root)
    assert root.is_dir()


async def test_accepts_a_string_root(tmp_path: Path) -> None:
    store = FileSystemBlobStore(str(tmp_path / "blobs"))
    await store.put("k", b"data")
    assert await store.get("k") == b"data"


async def test_list_reports_metadata(tmp_path: Path) -> None:
    store = FileSystemBlobStore(tmp_path / "blobs")
    await store.put("with-metadata", b"x", metadata={"sha256": "abc", "size": "1"})
    infos = [info async for info in store.list("with-metadata")]
    assert len(infos) == 1
    assert dict(infos[0].metadata) == {"sha256": "abc", "size": "1"}


async def test_blobs_are_content_shaped_not_key_shaped_on_disk(tmp_path: Path) -> None:
    """The on-disk layout never uses the caller's key as a path component."""
    root = tmp_path / "blobs"
    store = FileSystemBlobStore(root)
    await store.put("some/arbitrary/looking/key", b"data")
    paths = list(root.rglob("*.blob"))
    assert len(paths) == 1
    assert "some" not in str(paths[0].relative_to(root))


@pytest.mark.parametrize(
    "malicious_key",
    [
        "../../../etc/passwd",
        "/etc/passwd",
        "..",
        "a/../../b",
        "..\\..\\windows\\system32",
    ],
)
async def test_path_traversal_attempts_stay_inside_root(tmp_path: Path, malicious_key: str) -> None:
    root = tmp_path / "blobs"
    store = FileSystemBlobStore(root)
    await store.put(malicious_key, b"contained")

    # Nothing was written anywhere outside root.
    escaped = [path for path in tmp_path.rglob("*") if not path.is_relative_to(root)]
    assert escaped == []

    # ... and the store can still read back exactly what it was given, under the same key.
    assert await store.get(malicious_key) == b"contained"


async def test_write_is_atomic_no_temp_files_left_behind(tmp_path: Path) -> None:
    root = tmp_path / "blobs"
    store = FileSystemBlobStore(root)
    await store.put("k", b"data")
    leftover_temp_files = list(root.rglob(".tmp-*"))
    assert leftover_temp_files == []


async def test_delete_is_idempotent_across_repeated_calls(tmp_path: Path) -> None:
    store = FileSystemBlobStore(tmp_path / "blobs")
    await store.put("k", b"data")
    await store.delete("k")
    await store.delete("k")  # must not raise the second time either
