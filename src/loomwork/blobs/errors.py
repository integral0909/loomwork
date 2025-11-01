"""Errors raised by blob stores.

These describe failures in the blob storage layer itself, independent of anything the
payload codec chain does with it (see ``loomwork.codec.offload`` for the codec that maps
:class:`BlobNotFoundError` onto a clear, key-naming failure and reserves
:class:`~loomwork.codec.errors.IntegrityError` for a blob whose content does not match
its recorded hash).
"""

from __future__ import annotations

from loomwork.errors import LoomworkError


class BlobStoreError(LoomworkError):
    """Base class for blob store failures."""


class BlobNotFoundError(BlobStoreError):
    """No blob exists under the given key."""

    def __init__(self, key: str) -> None:
        super().__init__(f"no blob found for key {key!r}")
        self.key = key
