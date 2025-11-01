"""Garbage collection safety: referenced blobs are never deleted, young blobs are never
deleted regardless of whether anything references them yet, and only old, unreferenced
blobs actually go.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from loomwork.blobs.gc import GcReport, collect_garbage
from loomwork.blobs.memory import MemoryBlobStore
from loomwork.codec.offload import OffloadCodec
from loomwork.codec.payload import ENCODING_BYTES, ENCODING_KEY, Payload
from loomwork.history.events import StepCompleted, WorkflowStarted
from loomwork.store.memory import MemoryStore
from loomwork.store.records import ExecutionRecord, ExecutionStatus

NOW = datetime(2024, 6, 1, tzinfo=UTC)
GRACE = timedelta(hours=24)


def _backdate(blob_store: MemoryBlobStore, key: str, when: datetime) -> None:
    """Rewrite a blob's recorded write time, so age-based tests don't depend on real
    wall-clock sleeps."""
    data, metadata, _ = blob_store._blobs[key]
    blob_store._blobs[key] = (data, metadata, when)


async def _offload(blob_store: MemoryBlobStore, data: bytes) -> Payload:
    codec = OffloadCodec(blob_store, threshold_bytes=1)
    (encoded,) = await codec.encode([Payload(data, {ENCODING_KEY: ENCODING_BYTES})])
    return encoded


async def _create_run(
    store: MemoryStore,
    run_id: str,
    result: Payload,
    *,
    status: ExecutionStatus = ExecutionStatus.RUNNING,
    created_at: datetime = NOW - timedelta(days=10),
) -> None:
    record = ExecutionRecord(
        run_id=run_id,
        workflow_id=f"wf-{run_id}",
        workflow_name="wf",
        status=status,
        created_at=created_at,
    )
    started = WorkflowStarted(workflow_name="wf", workflow_id=record.workflow_id, run_id=run_id)
    await store.create_execution(record, started, created_at)
    appended = await store.append_input(run_id, StepCompleted(seq=0, result=result), created_at)
    assert appended is not None, "run must still be open to append its result"


async def test_referenced_blob_survives_even_when_old() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    result = await _offload(blob_store, b"x" * 1000)
    key = result.metadata["codec.offload.key"]
    _backdate(blob_store, key, NOW - timedelta(days=30))
    await _create_run(store, "r1", result)

    report = await collect_garbage(blob_store, store, grace_period=GRACE, now=NOW)

    assert await blob_store.exists(key)
    assert report.referenced_blobs == 1
    assert report.deleted_blobs == 0


async def test_unreferenced_old_blob_is_deleted() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    await blob_store.put("orphan", b"nobody references this")
    _backdate(blob_store, "orphan", NOW - timedelta(days=30))

    report = await collect_garbage(blob_store, store, grace_period=GRACE, now=NOW)

    assert not await blob_store.exists("orphan")
    assert report.deleted_blobs == 1
    assert report.deleted_bytes == len(b"nobody references this")


async def test_unreferenced_fresh_blob_survives_the_grace_period() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    await blob_store.put("too-fresh-to-tell", b"data")
    # No backdating: this blob was "just written" relative to `now`.

    report = await collect_garbage(blob_store, store, grace_period=GRACE, now=NOW)

    assert await blob_store.exists("too-fresh-to-tell")
    assert report.skipped_within_grace_period == 1
    assert report.deleted_blobs == 0


async def test_dry_run_reports_without_deleting() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    await blob_store.put("orphan", b"data")
    _backdate(blob_store, "orphan", NOW - timedelta(days=30))

    report = await collect_garbage(blob_store, store, grace_period=GRACE, now=NOW, dry_run=True)

    assert await blob_store.exists("orphan"), "dry_run must not delete anything"
    assert report.deleted_blobs == 1, "but still reports what would have been deleted"


async def test_prefix_restricts_the_scan() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    await blob_store.put("keep/outside-prefix", b"data")
    await blob_store.put("scan/inside-prefix", b"data")
    _backdate(blob_store, "keep/outside-prefix", NOW - timedelta(days=30))
    _backdate(blob_store, "scan/inside-prefix", NOW - timedelta(days=30))

    report = await collect_garbage(blob_store, store, grace_period=GRACE, now=NOW, prefix="scan/")

    assert await blob_store.exists("keep/outside-prefix"), "outside the prefix: never considered"
    assert not await blob_store.exists("scan/inside-prefix")
    assert report.scanned_blobs == 1


async def test_running_executions_are_scanned_too() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    result = await _offload(blob_store, b"x" * 1000)
    key = result.metadata["codec.offload.key"]
    _backdate(blob_store, key, NOW - timedelta(days=30))
    await _create_run(store, "still-running", result, status=ExecutionStatus.RUNNING)

    report = await collect_garbage(blob_store, store, grace_period=GRACE, now=NOW)

    assert await blob_store.exists(key)
    assert report.scanned_runs == 1


async def test_multiple_runs_each_contribute_references() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    keys = []
    for i in range(5):
        result = await _offload(blob_store, bytes([i]) * 1000)
        key = result.metadata["codec.offload.key"]
        _backdate(blob_store, key, NOW - timedelta(days=30))
        await _create_run(store, f"run-{i}", result, created_at=NOW - timedelta(days=10, hours=i))
        keys.append(key)

    report = await collect_garbage(blob_store, store, grace_period=GRACE, now=NOW)

    assert report.scanned_runs == 5
    assert report.referenced_blobs == 5
    for key in keys:
        assert await blob_store.exists(key)


async def test_default_now_is_real_time() -> None:
    blob_store = MemoryBlobStore()
    store = MemoryStore()
    await blob_store.put("fresh", b"data")  # written "now" -- must never be seen as old
    report = await collect_garbage(blob_store, store, grace_period=GRACE)
    assert await blob_store.exists("fresh")
    assert report.skipped_within_grace_period == 1


async def test_report_is_a_plain_dataclass_with_zero_defaults() -> None:
    assert GcReport() == GcReport(0, 0, 0, 0, 0, 0)


@pytest.mark.parametrize("page_size", [1, 2, 1000])
async def test_history_scan_is_paginated(page_size: int) -> None:
    """Regardless of how many history pages a run's offload references span, every one
    of them is found (exercising ``_referenced_keys_in_run``'s own paging, independent of
    ``collect_garbage``'s execution-level paging tested above)."""
    from loomwork.blobs.gc import _referenced_keys_in_run

    blob_store = MemoryBlobStore()
    store = MemoryStore()
    result_a = await _offload(blob_store, b"a" * 1000)
    result_b = await _offload(blob_store, b"b" * 1000)
    await _create_run(store, "paged", result_a)
    await store.append_input(
        "paged", StepCompleted(seq=1, result=result_b), NOW - timedelta(days=10)
    )

    keys = await _referenced_keys_in_run(store, "paged", page_size=page_size)

    assert keys == {
        result_a.metadata["codec.offload.key"],
        result_b.metadata["codec.offload.key"],
    }
