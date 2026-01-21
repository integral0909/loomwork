"""Large payload offloading ("claim check"): multi-megabyte step outputs never bloat
the workflow's history -- they are compressed, encrypted, then moved to a blob store,
leaving a small reference behind.

.. code-block:: console

    uv run python -m examples.media_processing.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import loomwork as lw
from examples.media_processing.steps import PLAINTEXT_MARKER, render_image, render_report
from examples.media_processing.workflows import render_job
from examples.support import rule
from loomwork.blobs import FileSystemBlobStore, collect_garbage
from loomwork.codec import CodecChain, CompressionCodec, DataConverter, EnvironmentKeyRing
from loomwork.codec.encryption import EncryptionCodec
from loomwork.codec.keys import generate_key
from loomwork.codec.limits import PayloadSizeLimitCodec
from loomwork.codec.offload import OffloadCodec, offload_key
from loomwork.history.events import StepCompleted, StepScheduled
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions

#: OffloadCodec's own default (see docs/large-payloads.md); spelled out here since the
#: demo's numbers below are chosen to comfortably clear it.
OFFLOAD_THRESHOLD = 128 * 1024


def _converter(blob_store: FileSystemBlobStore) -> DataConverter:
    # An environment-style key ring: the same shape a containerized deployment injects
    # as real environment variables (LOOMWORK_KEY_<id>, LOOMWORK_CURRENT_KEY -- see
    # docs/payload-encryption.md). Built from an explicit mapping (rather than the real
    # process environment) so the demo never depends on, or pollutes, os.environ.
    key_ring = EnvironmentKeyRing(
        env={"LOOMWORK_KEY_2024-q1": generate_key(), "LOOMWORK_CURRENT_KEY": "2024-q1"}
    )
    # OffloadCodec must be outermost -- last on encode (after compression and
    # encryption), first on decode -- so the blob store only ever holds ciphertext, and
    # PayloadSizeLimitCodec sees the small reference instead of the pre-offload data
    # (see docs/large-payloads.md's "Ordering in the codec chain").
    chain = CodecChain(
        [
            CompressionCodec(),
            EncryptionCodec(key_ring),
            OffloadCodec(blob_store, threshold_bytes=OFFLOAD_THRESHOLD),
            PayloadSizeLimitCodec(max_bytes=4096),
        ]
    )
    return DataConverter(codec=chain)


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "media.db"
        blob_dir = Path(tmp) / "blobs"
        blob_store = FileSystemBlobStore(blob_dir)

        async with SQLiteStore(db_path) as store:
            app = lw.Loomwork(
                store=store,
                workflows=[render_job],
                steps=[render_image, render_report],
                converter=_converter(blob_store),
                worker_options=WorkerOptions(shutdown_grace=0.5),
            )
            async with app:
                print(rule("Rendering an 810,000-pixel image, then a report referencing it"))
                handle = await app.start_workflow(render_job, "job-1", 900, 900, id="job-1")
                result = await handle.result(timeout=20)
                print(f"result: {result.model_dump()}")
                assert result.image_bytes == 900 * 900 * 3
                run_id = handle.run_id or ""

            def _describe(payload: lw.Payload) -> str:
                key = offload_key(payload)
                if key is None:
                    return f"{len(payload.data)}-byte payload, kept inline"
                return f"{len(payload.data)}-byte payload, offloaded to blob {key[:12]}..."

            print(rule("What actually reached the store's history"))
            for event in await store.load_history(run_id):
                if isinstance(event, StepScheduled) and event.args:
                    for arg in event.args:
                        print(f"  StepScheduled({event.step_name}): {_describe(arg)}")
                elif isinstance(event, StepCompleted):
                    print(f"  StepCompleted(seq={event.seq}): {_describe(event.result)}")

        print(rule("The blob directory holds ciphertext, never the plaintext marker"))
        blob_files = list(blob_dir.rglob("*.blob"))
        total_bytes = sum(path.stat().st_size for path in blob_files)
        print(f"blob files on disk: {len(blob_files)} ({total_bytes} bytes total)")
        marker_in_a_blob = any(PLAINTEXT_MARKER in path.read_bytes() for path in blob_files)
        print(f"plaintext marker present in any blob file: {marker_in_a_blob}")
        assert blob_files
        assert not marker_in_a_blob

        print(rule("Pruning the (now closed) run, then collecting the blobs it leaves orphaned"))
        async with SQLiteStore(db_path) as store:
            pruned = await store.delete_closed_before(
                datetime.now(UTC) + timedelta(seconds=1), limit=100
            )
            print(f"runs pruned: {pruned}")
            # A real GC job runs periodically, well after collect_garbage's own grace
            # period (24h by default) -- simulated here by dating the scan, rather than
            # shortening the grace period a production run would rely on.
            report = await collect_garbage(
                blob_store, store, now=datetime.now(UTC) + timedelta(hours=25)
            )
            print(f"garbage collection report: {report}")

        remaining = list(blob_dir.rglob("*.blob"))
        print(f"blob files remaining after collect_garbage: {len(remaining)}")
        assert not remaining


if __name__ == "__main__":
    asyncio.run(main())
