"""End-to-end archiving of real runs: writing, streaming reads, and cursor pagination."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from loomwork import Loomwork, ManualClock, MemoryStore
from loomwork.archive import (
    ArchiveError,
    ArchiveNotFoundError,
    ArchiveReader,
    ArchiveWriter,
    archive_and_prune,
)
from loomwork.errors import NotFoundError, WorkflowFailedError
from tests.archive import sample
from tests.helpers import eventually


async def _run_to_completion(
    app: Loomwork,
    workflow_id: str,
    text: str,
    *,
    search_attributes: dict[str, object] | None = None,
) -> str:
    handle = await app.start_workflow(
        sample.greet, text, id=workflow_id, search_attributes=search_attributes
    )
    result = await handle.result(timeout=5)
    assert result == text.upper()
    assert handle.run_id is not None
    return handle.run_id


async def test_round_trip_one_run(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    run_id = await _run_to_completion(app, "w1", "hello", search_attributes={"tier": "gold"})
    cutoff = clock.now() + timedelta(seconds=1)

    directory = tmp_path / "archive"
    manifest = await ArchiveWriter(directory).write(
        memory_store, cutoff=cutoff, source="memory://test"
    )
    assert manifest.run_count == 1
    assert len(manifest.segments) == 1
    assert manifest.segments[0].run_count == 1
    assert manifest.time_range is not None

    reader = ArchiveReader(directory)
    verification = reader.verify()
    assert verification.run_count == 1
    assert verification.segment_count == 1

    runs = list(reader.iter_runs())
    assert len(runs) == 1
    run = runs[0]
    assert run.record.workflow_id == "w1"
    assert run.record.run_id == run_id
    assert run.record.status.value == "completed"
    assert run.search_attributes == {"tier": "gold"}
    expected_history = await memory_store.load_history(run_id)
    assert [e.kind for e in run.history] == [e.kind for e in expected_history]
    assert [e.event_id for e in run.history] == [e.event_id for e in expected_history]

    assert reader.find(workflow_id="w1").record.run_id == run_id
    assert reader.find(run_id=run_id).record.run_id == run_id


async def test_only_archives_closed_runs_before_cutoff(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    old_run = await _run_to_completion(app, "old", "old")
    cutoff = clock.now() + timedelta(seconds=1)

    # Closed *after* the cutoff moment: must not be archived.
    await clock.advance(timedelta(hours=2))
    new_run = await _run_to_completion(app, "new", "new")

    # Still running: must not be archived regardless of age.
    running_handle = await app.start_workflow(sample.sleeper, "asleep", id="running")
    await eventually(lambda: memory_store.timer_count >= 1)

    manifest = await ArchiveWriter(tmp_path / "a").write(memory_store, cutoff=cutoff, source="s")
    assert manifest.run_count == 1
    [run] = list(ArchiveReader(tmp_path / "a").iter_runs())
    assert run.record.run_id == old_run
    assert run.record.run_id != new_run
    assert run.record.workflow_id != "running"
    assert running_handle.run_id  # still open, untouched


async def test_query_filters_which_runs_are_archived(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    await _run_to_completion(app, "ok", "ok")
    failed_handle = await app.start_workflow(sample.failer, "boom", id="broken")
    with pytest.raises(WorkflowFailedError):
        await failed_handle.result(timeout=5)
    cutoff = clock.now() + timedelta(seconds=1)

    manifest = await ArchiveWriter(tmp_path / "a").write(
        memory_store, cutoff=cutoff, source="s", query="status = 'failed'"
    )
    assert manifest.run_count == 1
    [run] = list(ArchiveReader(tmp_path / "a").iter_runs())
    assert run.record.workflow_id == "broken"
    assert run.record.status.value == "failed"


async def test_segment_rotation_and_pagination(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    for i in range(8):
        await _run_to_completion(app, f"w{i}", f"text{i}")
    cutoff = clock.now() + timedelta(seconds=1)

    directory = tmp_path / "archive"
    # A tiny segment size forces rotation after (about) every run, and a tiny batch size
    # forces the cursor-paginated store selection to run several rounds.
    manifest = await ArchiveWriter(directory, segment_max_bytes=200).write(
        memory_store, cutoff=cutoff, source="s", batch_size=3
    )
    assert manifest.run_count == 8
    assert len(manifest.segments) > 1
    for segment in manifest.segments:
        assert segment.run_count >= 1

    reader = ArchiveReader(directory)
    reader.verify()
    found_ids = [r.record.workflow_id for r in reader.iter_runs()]
    assert sorted(found_ids) == sorted(f"w{i}" for i in range(8))
    assert len(found_ids) == len(set(found_ids))


async def test_iter_runs_filters_by_workflow_name(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    await _run_to_completion(app, "a", "a")
    failed_handle = await app.start_workflow(sample.failer, "boom", id="b")
    with pytest.raises(WorkflowFailedError):
        await failed_handle.result(timeout=5)
    cutoff = clock.now() + timedelta(seconds=1)
    await ArchiveWriter(tmp_path / "a").write(memory_store, cutoff=cutoff, source="s")

    reader = ArchiveReader(tmp_path / "a")
    only_greet = list(reader.iter_runs(workflow_name="arc.wf"))
    assert [r.record.workflow_id for r in only_greet] == ["a"]


async def test_find_missing_run_raises(
    tmp_path: Path, make_app, memory_store: MemoryStore, clock: ManualClock
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    await _run_to_completion(app, "a", "a")
    cutoff = clock.now() + timedelta(seconds=1)
    await ArchiveWriter(tmp_path / "a").write(memory_store, cutoff=cutoff, source="s")

    reader = ArchiveReader(tmp_path / "a")
    with pytest.raises(ArchiveNotFoundError):
        reader.find(run_id="does-not-exist")
    with pytest.raises(ArchiveNotFoundError):
        reader.find(workflow_id="does-not-exist")


async def test_archive_and_prune_deletes_only_archived_runs(
    make_app, memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    app = await make_app(sample.WORKFLOWS, sample.STEPS, store=memory_store)
    old_run = await _run_to_completion(app, "old", "old")
    running_handle = await app.start_workflow(sample.sleeper, "asleep", id="running")
    await eventually(lambda: memory_store.timer_count >= 1)
    cutoff = clock.now() + timedelta(seconds=1)

    result = await archive_and_prune(
        memory_store, tmp_path / "a", cutoff=cutoff, source="s", prune=True
    )
    assert result.manifest.run_count == 1
    assert result.pruned == 1
    with pytest.raises(NotFoundError):
        await memory_store.get_execution(old_run)
    # The still-running workflow was never touched.
    record = await memory_store.get_execution(running_handle.run_id)
    assert record.status.value == "running"


async def test_prune_rejects_query_filter(
    memory_store: MemoryStore, clock: ManualClock, tmp_path: Path
):
    cutoff = clock.now() + timedelta(seconds=1)
    with pytest.raises(ArchiveError, match="query"):
        await archive_and_prune(
            memory_store,
            tmp_path / "a",
            cutoff=cutoff,
            source="s",
            query="status = 'failed'",
            prune=True,
        )
    assert not (tmp_path / "a" / "manifest.json").exists()


async def test_refuses_to_write_into_a_non_empty_directory(tmp_path: Path) -> None:
    from datetime import UTC, datetime

    from loomwork.archive.errors import ArchiveError
    from loomwork.archive.writer import ArchiveWriter

    target = tmp_path / "archive"
    target.mkdir()
    (target / "manifest.json").write_text("{}")
    with pytest.raises(ArchiveError, match="not empty"):
        await ArchiveWriter(target).write(
            MemoryStore(), cutoff=datetime.now(UTC), source="memory://"
        )
    assert (target / "manifest.json").read_text() == "{}"
