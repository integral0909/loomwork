"""Reading archives: streaming iteration, lookup, and checksum verification."""

from __future__ import annotations

import gzip
import hashlib
import json
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from loomwork.archive.errors import ArchiveCorruptionError, ArchiveNotFoundError
from loomwork.archive.format import (
    MANIFEST_FILENAME,
    SEGMENT_PREFIX,
    SEGMENT_SUFFIX,
    ArchivedRun,
    ArchiveManifest,
    SegmentManifest,
)
from loomwork.history.serde import HistoryFormatError

_HASH_CHUNK = 1024 * 1024


def _read_manifest(directory: Path) -> ArchiveManifest:
    path = directory / MANIFEST_FILENAME
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ArchiveCorruptionError(f"{directory}: no {MANIFEST_FILENAME} found") from None
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ArchiveCorruptionError(f"{path}: corrupt JSON ({exc})") from exc
    try:
        return ArchiveManifest.from_dict(data)
    except (KeyError, TypeError, ValueError) as exc:
        raise ArchiveCorruptionError(f"{path}: malformed manifest ({exc})") from exc


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_HASH_CHUNK):
            hasher.update(chunk)
    return hasher.hexdigest()


@dataclass(frozen=True, slots=True)
class ArchiveVerification:
    """Result of a successful :meth:`ArchiveReader.verify`."""

    segment_count: int
    run_count: int


class ArchiveReader:
    """Reads an archive directory written by :class:`~loomwork.archive.writer.ArchiveWriter`.

    Iteration is streaming: each segment is decompressed and parsed one line at a time,
    so reading an archive never holds more than one run's history in memory regardless
    of how many runs it contains.
    """

    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory)
        self._manifest: ArchiveManifest | None = None

    @property
    def manifest(self) -> ArchiveManifest:
        if self._manifest is None:
            self._manifest = _read_manifest(self.directory)
        return self._manifest

    def iter_runs(self, *, workflow_name: str | None = None) -> Iterator[ArchivedRun]:
        """Every archived run, oldest segment first, in the order it was written."""
        for segment in self.manifest.segments:
            for run in self._iter_segment(segment):
                if workflow_name is not None and run.record.workflow_name != workflow_name:
                    continue
                yield run

    def find(self, *, workflow_id: str | None = None, run_id: str | None = None) -> ArchivedRun:
        """The archived run matching ``run_id``, or else the latest with ``workflow_id``.

        Raises:
            ArchiveNotFoundError: no run matches.
        """
        if run_id is None and workflow_id is None:
            raise ValueError("give workflow_id or run_id")
        best: ArchivedRun | None = None
        for run in self.iter_runs():
            if run_id is not None:
                if run.record.run_id == run_id:
                    return run
                continue
            if run.record.workflow_id == workflow_id and (
                best is None or run.record.created_at >= best.record.created_at
            ):
                best = run
        if best is not None:
            return best
        target = run_id or workflow_id
        raise ArchiveNotFoundError(f"no archived run matches {target!r} in {self.directory}")

    def verify(self) -> ArchiveVerification:
        """Recompute every segment's checksum and structure; raise on the first problem.

        Checks, per segment: the file exists, its byte size and SHA-256 match the
        manifest, and every line decompresses and parses into a valid archived run,
        with the count matching the manifest. Finally checks that no segment file on
        disk is missing from the manifest (a sign the manifest is stale).
        """
        manifest = self.manifest
        seen: set[str] = set()
        for segment in manifest.segments:
            path = self.directory / segment.filename
            if not path.is_file():
                raise ArchiveCorruptionError(
                    f"segment {segment.filename!r} is listed in the manifest but missing "
                    f"from {self.directory}"
                )
            seen.add(segment.filename)
            size = path.stat().st_size
            if size != segment.byte_size:
                raise ArchiveCorruptionError(
                    f"{segment.filename}: size is {size} bytes, manifest says {segment.byte_size}"
                    " (truncated or modified after archiving)"
                )
            digest = _sha256_file(path)
            if digest != segment.sha256:
                raise ArchiveCorruptionError(
                    f"{segment.filename}: checksum {digest} does not match manifest "
                    f"{segment.sha256} (the file was altered or corrupted)"
                )
            count = sum(1 for _ in self._iter_segment(segment))
            if count != segment.run_count:
                raise ArchiveCorruptionError(
                    f"{segment.filename}: manifest says {segment.run_count} run(s), found {count}"
                )
        extra = sorted(
            p.name
            for p in self.directory.glob(f"{SEGMENT_PREFIX}*{SEGMENT_SUFFIX}")
            if p.name not in seen
        )
        if extra:
            raise ArchiveCorruptionError(
                f"segment file(s) present but not listed in the manifest: {extra}"
            )
        return ArchiveVerification(
            segment_count=len(manifest.segments), run_count=manifest.run_count
        )

    # ------------------------------------------------------------------ internals

    def _iter_segment(self, segment: SegmentManifest) -> Iterator[ArchivedRun]:
        path = self.directory / segment.filename
        if not path.is_file():
            raise ArchiveCorruptionError(
                f"segment {segment.filename!r} is listed in the manifest but missing "
                f"from {self.directory}"
            )
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                for line_number, line in enumerate(fh, start=1):
                    if not line.strip():
                        continue
                    yield _parse_line(segment.filename, line_number, line)
        except EOFError as exc:
            raise ArchiveCorruptionError(
                f"{segment.filename}: truncated gzip stream ({exc})"
            ) from exc
        except gzip.BadGzipFile as exc:
            raise ArchiveCorruptionError(f"{segment.filename}: not a gzip file ({exc})") from exc
        except zlib.error as exc:
            raise ArchiveCorruptionError(
                f"{segment.filename}: corrupt compressed data ({exc})"
            ) from exc


def _parse_line(filename: str, line_number: int, line: str) -> ArchivedRun:
    try:
        data = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ArchiveCorruptionError(f"{filename}:{line_number}: corrupt JSON ({exc})") from exc
    try:
        return ArchivedRun.from_dict(data)
    except (KeyError, TypeError, ValueError, HistoryFormatError) as exc:
        raise ArchiveCorruptionError(
            f"{filename}:{line_number}: malformed archived run ({exc})"
        ) from exc
