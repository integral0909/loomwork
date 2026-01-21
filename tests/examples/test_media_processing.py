"""Tests for ``examples.media_processing``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import loomwork as lw
from examples.media_processing.main import OFFLOAD_THRESHOLD, _converter
from examples.media_processing.steps import PLAINTEXT_MARKER, render_image, render_report
from examples.media_processing.workflows import render_job
from loomwork.blobs import FileSystemBlobStore, collect_garbage
from loomwork.codec.offload import offload_key
from loomwork.history.events import StepCompleted, StepScheduled
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions


async def test_large_payloads_are_offloaded_and_never_touch_the_blob_store_as_plaintext(
    tmp_path: Path,
) -> None:
    blob_store = FileSystemBlobStore(tmp_path / "blobs")
    async with SQLiteStore(tmp_path / "media.db") as store:
        app = lw.Loomwork(
            store=store,
            workflows=[render_job],
            steps=[render_image, render_report],
            converter=_converter(blob_store),
            worker_options=WorkerOptions(shutdown_grace=0.5),
        )
        async with app:
            handle = await app.start_workflow(render_job, "job-1", 900, 900, id="job-1")
            result = await handle.result(timeout=20)

        assert result.image_bytes == 900 * 900 * 3
        assert result.image_bytes > OFFLOAD_THRESHOLD

        history = await store.load_history(handle.run_id or "")
        completions = [e for e in history if isinstance(e, StepCompleted)]
        assert len(completions) == 2
        for event in completions:
            # The large result was offloaded: what actually reached the history is a
            # reference (empty data), not the multi-megabyte value itself.
            assert offload_key(event.result) is not None
            assert len(event.result.data) < 1024

        scheduled_with_large_args = [
            e for e in history if isinstance(e, StepScheduled) and e.step_name.endswith("report")
        ]
        (report_call,) = scheduled_with_large_args
        # render_report's second argument is the multi-megabyte image -- also offloaded.
        assert offload_key(report_call.args[1]) is not None

    blob_files = list((tmp_path / "blobs").rglob("*.blob"))
    assert blob_files
    for path in blob_files:
        assert PLAINTEXT_MARKER not in path.read_bytes()


async def test_pruning_and_collect_garbage_removes_orphaned_blobs(tmp_path: Path) -> None:
    blob_dir = tmp_path / "blobs"
    blob_store = FileSystemBlobStore(blob_dir)
    db_path = tmp_path / "media_gc.db"
    async with SQLiteStore(db_path) as store:
        app = lw.Loomwork(
            store=store,
            workflows=[render_job],
            steps=[render_image, render_report],
            converter=_converter(blob_store),
            worker_options=WorkerOptions(shutdown_grace=0.5),
        )
        async with app:
            handle = await app.start_workflow(render_job, "job-2", 900, 900, id="job-2")
            await handle.result(timeout=20)

    assert list(blob_dir.rglob("*.blob"))

    async with SQLiteStore(db_path) as store:
        pruned = await store.delete_closed_before(
            datetime.now(UTC) + timedelta(seconds=1), limit=100
        )
        assert pruned == 1
        report = await collect_garbage(
            blob_store, store, now=datetime.now(UTC) + timedelta(hours=25)
        )

    assert report.deleted_blobs == 3  # image result, report argument, report result
    assert report.referenced_blobs == 0  # the pruned run no longer references anything
    assert not list(blob_dir.rglob("*.blob"))
