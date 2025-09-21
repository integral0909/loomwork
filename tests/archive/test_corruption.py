"""Detecting damaged archives: flipped bytes, truncation, and manifest/segment mismatches."""

from __future__ import annotations

import gzip
import json
from datetime import timedelta
from pathlib import Path

import pytest

from loomwork import ManualClock, MemoryStore
from loomwork.archive import ArchiveCorruptionError, ArchiveReader, ArchiveWriter
from loomwork.archive.format import MANIFEST_FILENAME, ArchiveManifest
from tests.archive import sample


async def _archive_two_runs(
    make_app, memory_store: MemoryStore, clock: ManualClock, directory: Path
) -> ArchiveManifest:
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    for i in range(2):
        handle = await app.start_workflow(sample.greet, f"text{i}", id=f"r{i}")
        await handle.result(timeout=5)
    cutoff = clock.now() + timedelta(seconds=1)
    return await ArchiveWriter(directory).write(memory_store, cutoff=cutoff, source="s")


def _segment_path(directory: Path, manifest: ArchiveManifest) -> Path:
    return directory / manifest.segments[0].filename


async def test_flipped_byte_fails_checksum_verification(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    directory = tmp_path / "archive"
    manifest = await _archive_two_runs(make_app, memory_store, clock, directory)
    path = _segment_path(directory, manifest)

    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError, match="checksum"):
        reader.verify()


async def test_flipped_byte_fails_streaming_read(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    directory = tmp_path / "archive"
    manifest = await _archive_two_runs(make_app, memory_store, clock, directory)
    path = _segment_path(directory, manifest)

    data = bytearray(path.read_bytes())
    # Flip a byte inside the compressed payload (skip the gzip header) so a plain read
    # notices it too, not only the checksum comparison in verify().
    data[40] ^= 0xFF
    path.write_bytes(bytes(data))

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError):
        list(reader.iter_runs())


async def test_truncated_segment_is_detected(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    directory = tmp_path / "archive"
    manifest = await _archive_two_runs(make_app, memory_store, clock, directory)
    path = _segment_path(directory, manifest)

    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError):
        reader.verify()
    with pytest.raises(ArchiveCorruptionError):
        list(ArchiveReader(directory).iter_runs())


async def test_missing_segment_file_is_detected(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    directory = tmp_path / "archive"
    manifest = await _archive_two_runs(make_app, memory_store, clock, directory)
    _segment_path(directory, manifest).unlink()

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError, match="missing"):
        reader.verify()
    with pytest.raises(ArchiveCorruptionError, match="missing"):
        list(ArchiveReader(directory).iter_runs())


async def test_extra_segment_not_in_manifest_is_detected(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    directory = tmp_path / "archive"
    await _archive_two_runs(make_app, memory_store, clock, directory)
    stray = directory / "segment-99999.jsonl.gz"
    with gzip.open(stray, "wt", encoding="utf-8") as fh:
        fh.write("{}\n")

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError, match="not listed"):
        reader.verify()


async def test_missing_manifest_is_detected(tmp_path: Path):
    directory = tmp_path / "empty"
    directory.mkdir()
    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError, match=MANIFEST_FILENAME):
        reader.verify()
    with pytest.raises(ArchiveCorruptionError):
        list(reader.iter_runs())


async def test_size_mismatch_without_checksum_change_is_still_size_checked(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    """Appending bytes after the real gzip stream changes size but may leave a stale,
    still-parseable stream in front of it; the size check catches it independently of
    whether the extra bytes happen to change what a naive read would return."""
    directory = tmp_path / "archive"
    manifest = await _archive_two_runs(make_app, memory_store, clock, directory)
    path = _segment_path(directory, manifest)
    with path.open("ab") as fh:
        fh.write(b"\x00" * 16)

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError, match="size"):
        reader.verify()


async def test_corrupt_json_line_is_detected(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    directory = tmp_path / "archive"
    manifest = await _archive_two_runs(make_app, memory_store, clock, directory)
    path = _segment_path(directory, manifest)
    lines = gzip.decompress(path.read_bytes()).splitlines()
    lines[0] = lines[0][:-3]  # truncate the JSON object itself, still valid gzip
    with gzip.open(path, "wb") as fh:
        fh.write(b"\n".join(lines) + b"\n")

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError):
        list(reader.iter_runs())


async def test_malformed_run_record_is_detected(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    directory = tmp_path / "archive"
    manifest = await _archive_two_runs(make_app, memory_store, clock, directory)
    path = _segment_path(directory, manifest)
    lines = gzip.decompress(path.read_bytes()).decode("utf-8").splitlines()
    broken = json.loads(lines[0])
    del broken["record"]
    lines[0] = json.dumps(broken)
    with gzip.open(path, "wb") as fh:
        fh.write(("\n".join(lines) + "\n").encode("utf-8"))

    reader = ArchiveReader(directory)
    with pytest.raises(ArchiveCorruptionError):
        list(reader.iter_runs())
