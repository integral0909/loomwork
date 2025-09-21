"""Crash-safety of "archive then prune": pruning must never run ahead of a durable archive.

:func:`~loomwork.archive.writer.archive_and_prune` only prunes after ``ArchiveWriter``
has fully written and fsynced the archive *and* it has been independently re-read and
checksum-verified. These tests simulate a crash at each of those boundaries and check
that either everything survives in the store, or the archive is already durable enough
that nothing is lost.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from loomwork import Loomwork, ManualClock, MemoryStore
from loomwork.archive import ArchiveError, ArchiveReader, ArchiveWriter, archive_and_prune
from loomwork.archive.format import MANIFEST_FILENAME
from loomwork.history.events import Event
from tests.archive import sample


async def _closed_runs(app: Loomwork, count: int) -> list[str]:
    run_ids = []
    for i in range(count):
        handle = await app.start_workflow(sample.greet, f"r{i}", id=f"crash-{i}")
        await handle.result(timeout=5)
        assert handle.run_id is not None
        run_ids.append(handle.run_id)
    return run_ids


async def test_writer_failure_leaves_store_untouched_and_no_manifest(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path, monkeypatch
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    run_ids = await _closed_runs(app, 3)
    cutoff = clock.now() + timedelta(seconds=1)

    calls = 0
    original_load_history = memory_store.load_history

    async def flaky_load_history(
        run_id: str, after_id: int = 0, limit: int | None = None
    ) -> list[Event]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated crash while archiving")
        return await original_load_history(run_id, after_id, limit)

    monkeypatch.setattr(memory_store, "load_history", flaky_load_history)

    directory = tmp_path / "archive"
    with pytest.raises(RuntimeError, match="simulated crash"):
        await ArchiveWriter(directory).write(memory_store, cutoff=cutoff, source="memory://test")

    # No manifest: an incomplete archive is never mistaken for a usable one.
    assert not (directory / MANIFEST_FILENAME).exists()
    # No partial segment files were left behind either.
    assert list(directory.glob("segment-*")) == []
    # Every run recorded before the crash is exactly as it was: nothing was pruned.
    for run_id in run_ids:
        record = await memory_store.get_execution(run_id)
        assert record.status.value == "completed"


async def test_archive_and_prune_never_deletes_when_the_write_fails(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path, monkeypatch
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    run_ids = await _closed_runs(app, 2)
    cutoff = clock.now() + timedelta(seconds=1)

    delete_calls = 0
    original_delete = memory_store.delete_closed_before

    async def counting_delete(cutoff_arg: datetime, limit: int = 1000) -> int:
        nonlocal delete_calls
        delete_calls += 1
        return await original_delete(cutoff_arg, limit)

    async def failing_load_history(
        run_id: str, after_id: int = 0, limit: int | None = None
    ) -> list[Event]:
        raise RuntimeError("disk full")

    monkeypatch.setattr(memory_store, "delete_closed_before", counting_delete)
    monkeypatch.setattr(memory_store, "load_history", failing_load_history)

    with pytest.raises(RuntimeError, match="disk full"):
        await archive_and_prune(
            memory_store, tmp_path / "archive", cutoff=cutoff, source="s", prune=True
        )

    assert delete_calls == 0
    for run_id in run_ids:
        record = await memory_store.get_execution(run_id)
        assert record.status.value == "completed"


async def test_prune_skipped_when_post_write_verification_fails(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path, monkeypatch
):
    """Even after a fully successful write, a failed re-verification blocks pruning.

    This is the belt-and-braces check: ``archive_and_prune`` re-reads and verifies the
    archive from disk (not trusting its own in-memory result) before it ever calls
    ``delete_closed_before``.
    """
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    run_ids = await _closed_runs(app, 1)
    cutoff = clock.now() + timedelta(seconds=1)

    delete_calls = 0
    original_delete = memory_store.delete_closed_before

    async def counting_delete(cutoff_arg: datetime, limit: int = 1000) -> int:
        nonlocal delete_calls
        delete_calls += 1
        return await original_delete(cutoff_arg, limit)

    monkeypatch.setattr(memory_store, "delete_closed_before", counting_delete)

    def failing_verify(self: ArchiveReader) -> object:
        raise ArchiveError("simulated post-write corruption")

    monkeypatch.setattr(ArchiveReader, "verify", failing_verify)

    with pytest.raises(ArchiveError, match="simulated post-write corruption"):
        await archive_and_prune(
            memory_store, tmp_path / "archive", cutoff=cutoff, source="s", prune=True
        )

    assert delete_calls == 0
    for run_id in run_ids:
        record = await memory_store.get_execution(run_id)
        assert record.status.value == "completed"


async def test_partial_segment_is_discarded_not_left_corrupt(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path, monkeypatch
):
    """A crash mid-segment must not leave a segment file the manifest never mentions."""
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    await _closed_runs(app, 5)
    cutoff = clock.now() + timedelta(seconds=1)

    calls = 0
    original_load_history = memory_store.load_history

    async def flaky_load_history(
        run_id: str, after_id: int = 0, limit: int | None = None
    ) -> list[Event]:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("simulated crash")
        return await original_load_history(run_id, after_id, limit)

    monkeypatch.setattr(memory_store, "load_history", flaky_load_history)

    directory = tmp_path / "archive"
    with pytest.raises(RuntimeError, match="simulated crash"):
        await ArchiveWriter(directory, segment_max_bytes=1).write(
            memory_store, cutoff=cutoff, source="s"
        )
    assert not directory.exists() or list(directory.glob("segment-*")) == []
