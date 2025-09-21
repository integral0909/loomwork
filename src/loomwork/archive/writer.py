"""Streaming closed runs from a store into an archive.

:class:`ArchiveWriter` selects closed runs older than a cutoff (optionally narrowed by a
visibility query) in bounded batches, so archiving a store with millions of old runs
never holds more than one batch's histories in memory. :func:`archive_and_prune` adds the
"archive then delete" operation used by ``loomwork archive create --prune``; see its
docstring for the crash-safety argument.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from loomwork._version import __version__
from loomwork.archive.errors import ArchiveError
from loomwork.archive.format import (
    ARCHIVE_FORMAT_VERSION,
    MANIFEST_FILENAME,
    ArchivedRun,
    ArchiveManifest,
    SegmentManifest,
    segment_filename,
)
from loomwork.archive.reader import ArchiveReader
from loomwork.store.base import Store
from loomwork.visibility.schema import SearchAttributes

#: Default rotation threshold: a new segment starts once the current one reaches this
#: many compressed bytes (the last run written always finishes its segment first).
DEFAULT_SEGMENT_MAX_BYTES = 64 * 1024 * 1024

#: Runs (and their histories) held in memory at once while selecting from the store.
DEFAULT_BATCH_SIZE = 500


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_timestamp(value: datetime) -> str:
    return f"timestamp {_quote_string(value.isoformat())}"


class _HashingWriter:
    """Wraps a binary file object, hashing and counting every byte written to it."""

    def __init__(self, raw: BinaryIO) -> None:
        self._raw = raw
        self._hasher = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:
        self._hasher.update(data)
        self.size += len(data)
        return self._raw.write(data)

    def flush(self) -> None:
        self._raw.flush()

    @property
    def digest(self) -> str:
        return self._hasher.hexdigest()


class _SegmentWriter:
    """One open segment file: gzip-compressed JSONL, hashed and counted as it is written."""

    def __init__(self, directory: Path, index: int) -> None:
        self.filename = segment_filename(index)
        self._raw = open(directory / self.filename, "wb")  # noqa: SIM115 - closed in close()
        self._hashing = _HashingWriter(self._raw)
        self._gzip = gzip.GzipFile(fileobj=self._hashing, mode="wb")
        self.run_count = 0
        self.first_closed_at: datetime | None = None
        self.last_closed_at: datetime | None = None

    @property
    def size(self) -> int:
        return self._hashing.size

    def write(self, run: ArchivedRun) -> None:
        line = json.dumps(run.to_dict(), separators=(",", ":"), sort_keys=True)
        self._gzip.write(line.encode("utf-8"))
        self._gzip.write(b"\n")
        self._gzip.flush()
        self.run_count += 1
        closed = run.record.closed_at
        if closed is not None:
            self.first_closed_at = closed if self.first_closed_at is None else self.first_closed_at
            self.last_closed_at = closed

    def close(self) -> SegmentManifest:
        self._gzip.close()
        self._raw.flush()
        os.fsync(self._raw.fileno())
        self._raw.close()
        return SegmentManifest(
            filename=self.filename,
            run_count=self.run_count,
            byte_size=self._hashing.size,
            sha256=self._hashing.digest,
            first_closed_at=self.first_closed_at,
            last_closed_at=self.last_closed_at,
        )

    def discard(self) -> None:
        """Close and remove the file without recording it (nothing was ever written)."""
        self._gzip.close()
        self._raw.close()
        Path(self._raw.name).unlink(missing_ok=True)


def _fsync_path(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_manifest(directory: Path, manifest: ArchiveManifest) -> None:
    path = directory / MANIFEST_FILENAME
    data = json.dumps(manifest.to_dict(), indent=2, sort_keys=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


async def _select_closed_runs(
    store: Store,
    *,
    cutoff: datetime,
    query: str | None,
    schema: SearchAttributes,
    now: datetime,
    batch_size: int,
) -> AsyncIterator[ArchivedRun]:
    """Closed runs with ``closed_at < cutoff`` (and matching ``query``), oldest first.

    Paginated with a ``(closed_at, run_id)`` cursor rather than a growing ``LIMIT``
    offset, so no run is skipped or repeated even if many runs share a ``closed_at``.
    """
    cursor: tuple[datetime, str] | None = None
    while True:
        clauses = [f"closed_at < {_quote_timestamp(cutoff)}"]
        if query:
            clauses.append(f"({query})")
        if cursor is not None:
            cursor_closed_at, cursor_run_id = cursor
            clauses.append(
                f"(closed_at > {_quote_timestamp(cursor_closed_at)} OR "
                f"(closed_at = {_quote_timestamp(cursor_closed_at)} AND "
                f"run_id > {_quote_string(cursor_run_id)}))"
            )
        text = " AND ".join(clauses) + " ORDER BY closed_at ASC, run_id ASC"
        records = await store.query_executions(text, schema, now, limit=batch_size)
        if not records:
            return
        for record in records:
            history = await store.load_history(record.run_id)
            attributes = await store.get_search_attributes(record.run_id)
            yield ArchivedRun(record, attributes, history)
        last = records[-1]
        assert last.closed_at is not None  # the query only matches closed runs
        cursor = (last.closed_at, last.run_id)
        if len(records) < batch_size:
            return


class ArchiveWriter:
    """Writes closed runs into a rotating sequence of gzip-compressed JSONL segments."""

    def __init__(
        self, directory: Path | str, *, segment_max_bytes: int = DEFAULT_SEGMENT_MAX_BYTES
    ) -> None:
        if segment_max_bytes <= 0:
            raise ArchiveError("segment_max_bytes must be positive")
        self.directory = Path(directory)
        self.segment_max_bytes = segment_max_bytes

    async def write(
        self,
        store: Store,
        *,
        cutoff: datetime,
        source: str,
        query: str | None = None,
        schema: SearchAttributes | None = None,
        now: datetime | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> ArchiveManifest:
        """Archive every closed run with ``closed_at < cutoff`` (and matching ``query``).

        Segments are fsynced as they are closed and the manifest is written and fsynced
        last, after every segment; a crash at any point before this returns leaves no
        manifest, so the archive is simply incomplete and safe to discard or retry --
        nothing has been deleted from ``store`` yet. If this coroutine itself raises
        (the store failed, disk filled up, ...), every segment file written during this
        call -- closed or not -- is removed first, so a failed attempt leaves either a
        complete archive or an empty directory, never a partial one a later ``verify()``
        would have to puzzle over.
        """
        self.directory.mkdir(parents=True, exist_ok=True)
        if any(self.directory.iterdir()):
            # Segment names restart at 1 in every archive: writing into an existing one
            # would overwrite segments whose runs may already have been pruned.
            raise ArchiveError(
                f"{self.directory} is not empty; write each archive to a new directory"
            )
        schema = schema if schema is not None else SearchAttributes({})
        now = now if now is not None else datetime.now(UTC)

        segments: list[SegmentManifest] = []
        run_count = 0
        time_start: datetime | None = None
        time_end: datetime | None = None
        segment = _SegmentWriter(self.directory, len(segments) + 1)
        try:
            async for run in _select_closed_runs(
                store, cutoff=cutoff, query=query, schema=schema, now=now, batch_size=batch_size
            ):
                if segment.run_count and segment.size >= self.segment_max_bytes:
                    segments.append(segment.close())
                    segment = _SegmentWriter(self.directory, len(segments) + 1)
                segment.write(run)
                run_count += 1
                closed = run.record.closed_at
                if closed is not None:
                    time_start = closed if time_start is None else min(time_start, closed)
                    time_end = closed if time_end is None else max(time_end, closed)
            if segment.run_count:
                segments.append(segment.close())
            else:
                segment.discard()
        except BaseException:
            # Leave no partial archive behind: an aborted write must not leave segments
            # a manifest never gets written to describe, however durably they were
            # themselves fsynced.
            segment.discard()
            for closed_segment in segments:
                (self.directory / closed_segment.filename).unlink(missing_ok=True)
            raise

        time_range = None
        if time_start is not None and time_end is not None:
            time_range = (time_start, time_end)
        manifest = ArchiveManifest(
            format_version=ARCHIVE_FORMAT_VERSION,
            created_at=now,
            loomwork_version=__version__,
            source=source,
            run_count=run_count,
            segments=tuple(segments),
            time_range=time_range,
        )
        _write_manifest(self.directory, manifest)
        _fsync_path(self.directory)
        return manifest


@dataclass(frozen=True, slots=True)
class ArchiveAndPruneResult:
    manifest: ArchiveManifest
    pruned: int


async def archive_and_prune(
    store: Store,
    directory: Path | str,
    *,
    cutoff: datetime,
    source: str,
    query: str | None = None,
    schema: SearchAttributes | None = None,
    now: datetime | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    segment_max_bytes: int = DEFAULT_SEGMENT_MAX_BYTES,
    prune: bool = False,
    prune_batch: int = 1000,
) -> ArchiveAndPruneResult:
    """Archive closed runs, then (optionally) delete them from ``store``.

    Crash-safe ordering: pruning starts only after the archive has been fully written,
    fsynced (every segment, then the manifest, then the directory entry) *and*
    independently re-read and checksum-verified. A crash before that point leaves
    ``store`` completely untouched -- the run is still there, and the incomplete
    archive (missing its manifest, or verification failing) is simply not used.
    A crash during pruning leaves some already-archived runs still in the store; they
    are a strict subset of what the archive holds; re-running this (or plain
    ``loomwork store prune``) later deletes them without any risk of loss, since the
    archive already durably contains them. Nothing is ever deleted before it is durably
    archived, and nothing is archived twice into the same segment.

    ``prune`` is rejected together with ``query``: pruning always deletes every closed
    run older than ``cutoff``, which is a superset of what a filtered archive would have
    selected, so combining them could delete runs that were never archived.
    """
    if prune and query:
        raise ArchiveError(
            "--prune cannot be combined with a query filter: it would delete runs "
            "that a filtered archive never wrote"
        )
    writer = ArchiveWriter(directory, segment_max_bytes=segment_max_bytes)
    manifest = await writer.write(
        store,
        cutoff=cutoff,
        source=source,
        query=query,
        schema=schema,
        now=now,
        batch_size=batch_size,
    )
    pruned = 0
    if prune and manifest.run_count:
        ArchiveReader(directory).verify()
        while True:
            deleted = await store.delete_closed_before(cutoff, limit=prune_batch)
            pruned += deleted
            if deleted < prune_batch:
                break
    return ArchiveAndPruneResult(manifest, pruned)
