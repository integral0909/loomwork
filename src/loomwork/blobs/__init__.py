"""Blob storage for payloads too large to keep in a workflow history.

See ``loomwork.codec.offload`` for the codec that uses a :class:`BlobStore` to implement
"claim check" offloading, and ``loomwork.blobs.gc`` for garbage collecting the blobs it
writes. :class:`~loomwork.blobs.s3.S3BlobStore` needs the optional ``httpx`` dependency
(the ``s3`` extra); everything else here only needs the standard library. Importing this
package never requires ``httpx``, nor does it import ``loomwork.codec`` or
``loomwork.store`` -- ``S3BlobStore`` and everything in :mod:`loomwork.blobs.gc` (which
depends on both, to walk histories) are resolved lazily on first use, the same way
``loomwork.codec`` resolves ``EncryptionCodec``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loomwork.blobs.base import BlobInfo, BlobStore, ListableBlobStore
from loomwork.blobs.errors import BlobNotFoundError, BlobStoreError
from loomwork.blobs.filesystem import FileSystemBlobStore
from loomwork.blobs.memory import MemoryBlobStore
from loomwork.blobs.url import blob_store_from_url

if TYPE_CHECKING:
    from loomwork.blobs.gc import GcReport, collect_garbage
    from loomwork.blobs.s3 import S3BlobStore, S3Credentials

__all__ = [
    "BlobInfo",
    "BlobNotFoundError",
    "BlobStore",
    "BlobStoreError",
    "FileSystemBlobStore",
    "GcReport",
    "ListableBlobStore",
    "MemoryBlobStore",
    "S3BlobStore",
    "S3Credentials",
    "blob_store_from_url",
    "collect_garbage",
]

_GC_NAMES = ("GcReport", "collect_garbage")
_S3_NAMES = ("S3BlobStore", "S3Credentials")


def __getattr__(name: str) -> Any:
    if name in _S3_NAMES:
        from loomwork.blobs import s3

        return getattr(s3, name)
    if name in _GC_NAMES:
        from loomwork.blobs import gc

        return getattr(gc, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
