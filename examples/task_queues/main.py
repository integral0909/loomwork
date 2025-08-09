"""Render a video with two workers, sharing one SQLite store, routed by task queue.

.. code-block:: console

    uv run python -m examples.task_queues.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import loomwork as lw
from examples.support import rule
from examples.task_queues.steps import (
    extract_metadata,
    finalize_video,
    peak_concurrency,
    render_frame,
    timeline,
)
from examples.task_queues.workflows import render_video
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions
from loomwork.worker.limits import QueueLimit

FRAME_COUNT = 6


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "task_queues.db"
        print(rule(f"Two workers sharing {db_path.name}"))
        async with SQLiteStore(db_path) as store:
            general = lw.Loomwork(
                store=store,
                workflows=[render_video],
                steps=[extract_metadata, render_frame, finalize_video],
                worker_options=WorkerOptions(task_queues=("default",), shutdown_grace=0.5),
            )
            gpu = lw.Loomwork(
                store=store,
                workflows=[render_video],
                steps=[extract_metadata, render_frame, finalize_video],
                worker_options=WorkerOptions(
                    task_queues=("gpu",),
                    workflow_concurrency=0,  # this worker only claims steps, no ticks
                    queue_limits={"gpu": QueueLimit(concurrency=2)},
                    shutdown_grace=0.5,
                ),
            )
            async with general, gpu:
                print(
                    "general worker polls the 'default' queue; "
                    "gpu worker polls 'gpu' with concurrency=2"
                )
                result = await general.execute(
                    render_video, "vid-1", FRAME_COUNT, id="render-vid-1", timeout=20
                )
                print(f"result: {result}")

        print(rule("Where each step ran"))
        for queue, tag, started, ended in timeline:
            print(f"  [{queue:8s}] {tag:20s} took {ended - started:.3f}s")
        print(f"peak concurrent GPU renders: {peak_concurrency('gpu')} (limit was 2)")


if __name__ == "__main__":
    asyncio.run(main())
