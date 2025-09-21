"""Archiving closed workflow runs to cheap, self-describing storage.

``loomwork store prune`` deletes closed runs so a store does not grow forever, but
compliance and later debugging often need them kept somewhere. This package writes
closed runs to a directory of gzip-compressed JSON Lines segments plus a manifest
(:mod:`loomwork.archive.format`), streams them back out again
(:class:`~loomwork.archive.reader.ArchiveReader`), and combines archiving with pruning
in a crash-safe order (:func:`~loomwork.archive.writer.archive_and_prune`).

See ``docs/operations/archiving.md`` for the operational picture and the exact
crash-safety argument.
"""

from __future__ import annotations

from loomwork.archive.errors import ArchiveCorruptionError, ArchiveError, ArchiveNotFoundError
from loomwork.archive.format import (
    ARCHIVE_FORMAT_VERSION,
    ArchivedRun,
    ArchiveManifest,
    SegmentManifest,
)
from loomwork.archive.reader import ArchiveReader, ArchiveVerification
from loomwork.archive.writer import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_SEGMENT_MAX_BYTES,
    ArchiveAndPruneResult,
    ArchiveWriter,
    archive_and_prune,
)

__all__ = [
    "ARCHIVE_FORMAT_VERSION",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_SEGMENT_MAX_BYTES",
    "ArchiveAndPruneResult",
    "ArchiveCorruptionError",
    "ArchiveError",
    "ArchiveManifest",
    "ArchiveNotFoundError",
    "ArchiveReader",
    "ArchiveVerification",
    "ArchiveWriter",
    "ArchivedRun",
    "SegmentManifest",
    "archive_and_prune",
]
