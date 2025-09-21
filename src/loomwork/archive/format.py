"""The archive's on-disk format: a manifest plus rotating gzip-compressed JSONL segments.

An archive is a directory::

    manifest.json
    segment-00001.jsonl.gz
    segment-00002.jsonl.gz
    ...

Each segment holds one JSON object per line, one line per archived run
(:class:`ArchivedRun`): the run's execution record, its search attributes and its full
event history, exactly as read from the store. Payloads are archived *encoded* -- the
same bytes a store would have handed to a payload codec for decompression or
decryption -- so writing an archive never needs decryption keys and reading one back
does not either, unless the run is replayed as workflow code. This mirrors the exported
history documents ``loomwork workflow history --export`` produces
(:mod:`loomwork.history.serde`), just with a manifest and many runs per file.

The manifest is self-describing: format version, when and by which Loomwork version the
archive was written, a redacted description of the source store, the total run count,
the archive's overall time range, and one entry per segment with its own run count and
SHA-256 checksum. :class:`~loomwork.archive.reader.ArchiveReader` uses it to iterate
segments in order and to verify that nothing was altered or lost after the fact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import HISTORY_FORMAT_VERSION, Event
from loomwork.history.serde import HistoryFormatError, event_from_dict, event_to_dict
from loomwork.store.records import ExecutionRecord, ExecutionStatus

#: Bumped whenever an incompatible change is made to the archive's on-disk layout.
ARCHIVE_FORMAT_VERSION = 1

MANIFEST_FILENAME = "manifest.json"
SEGMENT_PREFIX = "segment-"
SEGMENT_SUFFIX = ".jsonl.gz"


def segment_filename(index: int) -> str:
    """``index`` is 1-based, matching the order segments were written in."""
    return f"{SEGMENT_PREFIX}{index:05d}{SEGMENT_SUFFIX}"


def record_to_dict(record: ExecutionRecord) -> dict[str, Any]:
    """Encode an :class:`ExecutionRecord` for a closed run.

    Fields that only matter to a running execution (lease ownership, tick scheduling)
    are not archived: :class:`ArchiveWriter` only ever archives closed runs, and
    :func:`record_from_dict` restores them at their natural defaults.
    """
    return {
        "run_id": record.run_id,
        "workflow_id": record.workflow_id,
        "workflow_name": record.workflow_name,
        "status": record.status.value,
        "created_at": record.created_at.isoformat(),
        "closed_at": record.closed_at.isoformat() if record.closed_at is not None else None,
        "last_event_id": record.last_event_id,
        "last_input_id": record.last_input_id,
        "consumed_through": record.consumed_through,
        "tick_failures": record.tick_failures,
        "last_tick_error": record.last_tick_error,
        "cancel_requested": record.cancel_requested,
        "result": record.result.to_dict() if record.result is not None else None,
        "failure": record.failure.to_dict() if record.failure is not None else None,
        "close_reason": record.close_reason,
        "parent_run_id": record.parent_run_id,
        "parent_seq": record.parent_seq,
        "parent_close_policy": record.parent_close_policy,
        "continued_run_id": record.continued_run_id,
    }


def record_from_dict(data: dict[str, Any]) -> ExecutionRecord:
    try:
        closed_at = data.get("closed_at")
        result = data.get("result")
        failure = data.get("failure")
        return ExecutionRecord(
            run_id=str(data["run_id"]),
            workflow_id=str(data["workflow_id"]),
            workflow_name=str(data["workflow_name"]),
            status=ExecutionStatus(data["status"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            closed_at=datetime.fromisoformat(closed_at) if closed_at else None,
            last_event_id=int(data.get("last_event_id", 0)),
            last_input_id=int(data.get("last_input_id", 0)),
            consumed_through=int(data.get("consumed_through", 0)),
            tick_failures=int(data.get("tick_failures", 0)),
            last_tick_error=data.get("last_tick_error"),
            cancel_requested=bool(data.get("cancel_requested", False)),
            result=Payload.from_dict(result) if result is not None else None,
            failure=FailureInfo.from_dict(failure) if failure is not None else None,
            close_reason=str(data.get("close_reason", "")),
            parent_run_id=data.get("parent_run_id"),
            parent_seq=data.get("parent_seq"),
            parent_close_policy=str(data.get("parent_close_policy", "terminate")),
            continued_run_id=data.get("continued_run_id"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HistoryFormatError(f"malformed execution record: {exc}") from exc


@dataclass(frozen=True, slots=True)
class ArchivedRun:
    """One archived run: its record, search attributes and complete history."""

    record: ExecutionRecord
    search_attributes: dict[str, Any]
    history: list[Event]

    def to_dict(self) -> dict[str, Any]:
        return {
            "record": record_to_dict(self.record),
            "search_attributes": dict(self.search_attributes),
            "history_format_version": HISTORY_FORMAT_VERSION,
            "history": [event_to_dict(e) for e in self.history],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArchivedRun:
        version = data.get("history_format_version")
        if version != HISTORY_FORMAT_VERSION:
            raise HistoryFormatError(
                f"unsupported history format version {version!r} "
                f"(expected {HISTORY_FORMAT_VERSION})"
            )
        try:
            history = [event_from_dict(item) for item in data["history"]]
        except KeyError as exc:
            raise HistoryFormatError(f"archived run is missing {exc}") from exc
        return cls(
            record=record_from_dict(data["record"]),
            search_attributes=dict(data.get("search_attributes", {})),
            history=history,
        )


@dataclass(frozen=True, slots=True)
class SegmentManifest:
    """One segment file's place in the manifest."""

    filename: str
    run_count: int
    byte_size: int
    sha256: str
    first_closed_at: datetime | None
    last_closed_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "run_count": self.run_count,
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "first_closed_at": (
                self.first_closed_at.isoformat() if self.first_closed_at is not None else None
            ),
            "last_closed_at": (
                self.last_closed_at.isoformat() if self.last_closed_at is not None else None
            ),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SegmentManifest:
        first = data.get("first_closed_at")
        last = data.get("last_closed_at")
        return cls(
            filename=str(data["filename"]),
            run_count=int(data["run_count"]),
            byte_size=int(data["byte_size"]),
            sha256=str(data["sha256"]),
            first_closed_at=datetime.fromisoformat(first) if first else None,
            last_closed_at=datetime.fromisoformat(last) if last else None,
        )


@dataclass(frozen=True, slots=True)
class ArchiveManifest:
    """Describes an archive directory: format, provenance, and one entry per segment."""

    format_version: int
    created_at: datetime
    loomwork_version: str
    source: str
    run_count: int
    segments: tuple[SegmentManifest, ...] = field(default_factory=tuple)
    time_range: tuple[datetime, datetime] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "created_at": self.created_at.isoformat(),
            "loomwork_version": self.loomwork_version,
            "source": self.source,
            "run_count": self.run_count,
            "time_range": (
                {"start": self.time_range[0].isoformat(), "end": self.time_range[1].isoformat()}
                if self.time_range is not None
                else None
            ),
            "segments": [s.to_dict() for s in self.segments],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArchiveManifest:
        time_range_data = data.get("time_range")
        time_range = None
        if time_range_data is not None:
            time_range = (
                datetime.fromisoformat(time_range_data["start"]),
                datetime.fromisoformat(time_range_data["end"]),
            )
        return cls(
            format_version=int(data["format_version"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            loomwork_version=str(data["loomwork_version"]),
            source=str(data["source"]),
            run_count=int(data["run_count"]),
            segments=tuple(SegmentManifest.from_dict(s) for s in data.get("segments", [])),
            time_range=time_range,
        )
