"""Content-addressed blob storage on a local (or network-mounted) filesystem.

Keys are never used as filesystem paths directly: the on-disk location of a blob is
derived by hashing the *key* (not its content -- callers such as the offload codec
already use content hashes as keys, but the store must not assume that), so a key
containing ``..``, an absolute path, or any other filesystem-meaningful character can
never escape ``root``. The original key is kept in a small sidecar file next to the
blob so :meth:`FileSystemBlobStore.list` can report it back.

Writes are crash-safe: data is written to a temporary file in the same directory as its
final name, fsynced, then atomically renamed into place (``os.replace``), and the
directory entry is itself fsynced afterwards. A crash at any point leaves either the old
content (if any) or nothing at that path -- never a partially written blob.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import tempfile
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from pathlib import Path

from loomwork.blobs.base import BlobInfo
from loomwork.blobs.errors import BlobNotFoundError

_BLOB_SUFFIX = ".blob"
_META_SUFFIX = ".meta.json"


def _digest(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write(path: Path, data: bytes) -> None:
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=directory, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise
    _fsync_dir(directory)


class FileSystemBlobStore:
    """Stores blobs under ``root`` as a two-level, hash-sharded directory tree."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _paths(self, key: str) -> tuple[Path, Path]:
        digest = _digest(key)
        directory = self.root / digest[:2] / digest[2:4]
        return directory / f"{digest}{_BLOB_SUFFIX}", directory / f"{digest}{_META_SUFFIX}"

    def _put_sync(self, key: str, data: bytes, metadata: dict[str, str]) -> None:
        blob_path, meta_path = self._paths(key)
        _atomic_write(blob_path, data)
        payload = json.dumps({"key": key, "metadata": metadata}).encode("utf-8")
        _atomic_write(meta_path, payload)

    async def put(self, key: str, data: bytes, metadata: Mapping[str, str] | None = None) -> None:
        await asyncio.to_thread(self._put_sync, key, bytes(data), dict(metadata or {}))

    async def get(self, key: str) -> bytes:
        blob_path, _ = self._paths(key)
        try:
            return await asyncio.to_thread(blob_path.read_bytes)
        except FileNotFoundError:
            raise BlobNotFoundError(key) from None

    async def delete(self, key: str) -> None:
        blob_path, meta_path = self._paths(key)
        await asyncio.to_thread(blob_path.unlink, missing_ok=True)
        await asyncio.to_thread(meta_path.unlink, missing_ok=True)

    async def exists(self, key: str) -> bool:
        blob_path, _ = self._paths(key)
        return await asyncio.to_thread(blob_path.exists)

    def _list_sync(self) -> list[BlobInfo]:
        out: list[BlobInfo] = []
        for meta_path in self.root.rglob(f"*{_META_SUFFIX}"):
            blob_path = meta_path.with_name(meta_path.name[: -len(_META_SUFFIX)] + _BLOB_SUFFIX)
            try:
                raw = json.loads(meta_path.read_text("utf-8"))
                stat = blob_path.stat()
            except (OSError, ValueError, KeyError):
                continue  # a concurrent delete, or a sidecar without its blob
            out.append(
                BlobInfo(
                    key=raw["key"],
                    size=stat.st_size,
                    last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                    metadata=raw.get("metadata", {}),
                )
            )
        return out

    async def list(self, prefix: str = "") -> AsyncIterator[BlobInfo]:
        for info in await asyncio.to_thread(self._list_sync):
            if info.key.startswith(prefix):
                yield info
