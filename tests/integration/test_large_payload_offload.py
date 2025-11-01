"""End-to-end: a step returning a large value, a real worker, and a history that stays
small because :class:`OffloadCodec` moved the bytes to a blob store."""

from __future__ import annotations

from pathlib import Path

import pytest

import loomwork as lw
from loomwork.blobs.gc import collect_garbage
from loomwork.blobs.memory import MemoryBlobStore
from loomwork.codec import CodecChain, DataConverter, PayloadSizeLimitCodec
from loomwork.codec.offload import ENCODING_OFFLOADED, OffloadCodec, offload_key
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG

pytestmark = pytest.mark.integration

ONE_MIB = 1024 * 1024
LARGE_REPORT = bytes(i % 256 for i in range(ONE_MIB))

#: Small enough that a 1 MiB payload can never sneak through unoffloaded, generous
#: enough that the small reference payload OffloadCodec produces always fits.
OFFLOAD_THRESHOLD = 64 * 1024
HISTORY_SIZE_LIMIT = 4096


@lw.step(name="large_payload.generate_report")
async def generate_report() -> bytes:
    return LARGE_REPORT


@lw.workflow(name="large_payload.build_report")
async def build_report() -> bytes:
    return await generate_report()


def converter(blob_store: MemoryBlobStore) -> DataConverter:
    return DataConverter(
        codec=CodecChain(
            [
                OffloadCodec(blob_store, threshold_bytes=OFFLOAD_THRESHOLD),
                PayloadSizeLimitCodec(max_bytes=HISTORY_SIZE_LIMIT),
            ]
        )
    )


async def test_large_step_result_is_offloaded_and_the_workflow_still_sees_it(
    tmp_path: Path, clock: lw.ManualClock
) -> None:
    blob_store = MemoryBlobStore()
    db_path = tmp_path / "offload.db"

    async with SQLiteStore(db_path) as store:
        app = lw.Loomwork(
            store=store,
            workflows=[build_report],
            steps=[generate_report],
            clock=clock,
            converter=converter(blob_store),
            config=FAST_CONFIG,
            worker_options=WorkerOptions(shutdown_grace=0.2),
        )
        async with app:
            handle = await app.start_workflow(build_report, id="report-1")
            result = await handle.result(timeout=10)

        assert result == LARGE_REPORT

        # The raw, stored history never held the megabyte-sized value: only a small
        # reference to it. This is what the codec chain is for -- a payload above the
        # limits.PayloadSizeLimitCodec threshold configured above would otherwise be
        # rejected outright, since OffloadCodec runs before it.
        history = await store.load_history(handle.run_id or "")
        step_completed = next(e for e in history if e.kind == "step_completed")
        assert step_completed.result.encoding == ENCODING_OFFLOADED  # type: ignore[attr-defined]
        stored_size = len(step_completed.result.data)  # type: ignore[attr-defined]
        assert stored_size < 500, f"expected a small reference, got {stored_size} bytes"

        key = offload_key(step_completed.result)  # type: ignore[attr-defined]
        assert key is not None
        assert await blob_store.get(key) == LARGE_REPORT

    # Replay: a fresh app (client-only, no worker) reading the same durable store and
    # blob store reconstructs the full value from history alone -- nothing about it
    # depends on the worker process that originally ran the step still being alive.
    async with (
        SQLiteStore(db_path) as reopened_store,
        lw.Loomwork(
            store=reopened_store, converter=converter(blob_store), run_worker=False
        ) as read_only,
    ):
        replayed = await read_only.get_handle(workflow_id="report-1").result(timeout=5)
        assert replayed == LARGE_REPORT


async def test_offloaded_blob_survives_garbage_collection_while_referenced(
    tmp_path: Path, clock: lw.ManualClock
) -> None:
    blob_store = MemoryBlobStore()
    db_path = tmp_path / "offload-gc.db"

    async with SQLiteStore(db_path) as store:
        app = lw.Loomwork(
            store=store,
            workflows=[build_report],
            steps=[generate_report],
            clock=clock,
            converter=converter(blob_store),
            config=FAST_CONFIG,
            worker_options=WorkerOptions(shutdown_grace=0.2),
        )
        async with app:
            handle = await app.start_workflow(build_report, id="report-gc")
            await handle.result(timeout=10)

        history = await store.load_history(handle.run_id or "")
        step_completed = next(e for e in history if e.kind == "step_completed")
        key = offload_key(step_completed.result)  # type: ignore[attr-defined]
        assert key is not None

        report = await collect_garbage(blob_store, store)

        assert report.referenced_blobs == 1
        assert await blob_store.exists(key), "GC must never delete a blob a run still references"
