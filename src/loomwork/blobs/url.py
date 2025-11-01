"""Build a :class:`~loomwork.blobs.base.BlobStore` from a connection string.

``file:///path``
    A :class:`~loomwork.blobs.filesystem.FileSystemBlobStore` rooted at ``/path``.
``s3://bucket/prefix?region=...&endpoint=...&path_style=...&sse=...``
    A :class:`~loomwork.blobs.s3.S3BlobStore`. Credentials are never taken from the URL
    (so they cannot end up logged alongside it): they always come from the standard
    ``AWS_*`` environment variables, via
    :func:`loomwork.blobs.s3.credentials_from_env`.
``memory://``
    A fresh :class:`~loomwork.blobs.memory.MemoryBlobStore` (process-local; the same URL
    used twice gives two independent stores, since there is nothing to look up by name).
"""

from __future__ import annotations

from urllib.parse import SplitResult, parse_qs, urlsplit

from loomwork.blobs.base import BlobStore
from loomwork.blobs.errors import BlobStoreError
from loomwork.blobs.filesystem import FileSystemBlobStore
from loomwork.blobs.memory import MemoryBlobStore

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


def blob_store_from_url(url: str) -> BlobStore:
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme == "memory":
        return MemoryBlobStore()
    if scheme == "file":
        return FileSystemBlobStore(_file_path(parts.netloc, parts.path))
    if scheme == "s3":
        return _s3_store_from_url(parts)
    raise BlobStoreError(f"unsupported blob store URL scheme {scheme!r} in {url!r}")


def _file_path(netloc: str, path: str) -> str:
    if netloc and netloc != "localhost":
        # file://host/path (relative to host) is not something we resolve; treat the
        # host component as the start of the path, as most "file://" parsers do.
        return f"/{netloc}{path}"
    return path or "/"


def _s3_store_from_url(parts: SplitResult) -> BlobStore:
    from loomwork.blobs.s3 import S3BlobStore  # lazy: optional httpx dependency

    bucket = parts.netloc
    if not bucket:
        raise BlobStoreError(f"s3 URL is missing a bucket name: {parts.geturl()!r}")
    prefix = parts.path.lstrip("/")
    params = {key: values[-1] for key, values in parse_qs(parts.query).items()}
    kwargs: dict[str, object] = {}
    if "region" in params:
        kwargs["region"] = params["region"]
    if "endpoint" in params:
        kwargs["endpoint_url"] = params["endpoint"]
    if "path_style" in params:
        kwargs["path_style"] = params["path_style"].lower() in _TRUE_VALUES
    if "sse" in params:
        kwargs["server_side_encryption"] = params["sse"]
    return S3BlobStore(bucket, prefix=prefix, **kwargs)  # type: ignore[arg-type]
