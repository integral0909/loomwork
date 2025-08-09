"""Tests for ``examples.task_queues``."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from examples.task_queues.steps import (
    GPU_QUEUE,
    extract_metadata,
    finalize_video,
    peak_concurrency,
    render_frame,
    timeline,
)
from examples.task_queues.workflows import render_video
from loomwork import Loomwork, StepDefinition
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions
from loomwork.worker.limits import QueueLimit

STEP_DEFS: list[StepDefinition[..., Any]] = [extract_metadata, render_frame, finalize_video]


@pytest.fixture(autouse=True)
def _reset_timeline() -> None:
    timeline.clear()


async def test_gpu_steps_only_run_on_the_gpu_worker(tmp_path: Path) -> None:
    async with SQLiteStore(tmp_path / "tq.db") as store:
        general = Loomwork(
            store=store,
            workflows=[render_video],
            steps=STEP_DEFS,
            worker_options=WorkerOptions(task_queues=("default",), shutdown_grace=0.5),
        )
        gpu = Loomwork(
            store=store,
            workflows=[render_video],
            steps=STEP_DEFS,
            worker_options=WorkerOptions(
                task_queues=(GPU_QUEUE,), workflow_concurrency=0, shutdown_grace=0.5
            ),
        )
        async with general, gpu:
            result = await general.execute(render_video, "vid-1", 3, timeout=10)

    assert result == "vid-1.1080p.mp4 (3 frames)"
    queues_used = {queue for queue, _tag, _start, _end in timeline}
    assert queues_used == {"default", GPU_QUEUE}
    gpu_tags = sorted(tag for queue, tag, _start, _end in timeline if queue == GPU_QUEUE)
    assert gpu_tags == [f"vid-1:frame-{i}" for i in range(3)]


async def test_gpu_queue_concurrency_limit_is_enforced(tmp_path: Path) -> None:
    async with SQLiteStore(tmp_path / "tq_limit.db") as store:
        general = Loomwork(
            store=store,
            workflows=[render_video],
            steps=STEP_DEFS,
            worker_options=WorkerOptions(task_queues=("default",), shutdown_grace=0.5),
        )
        gpu = Loomwork(
            store=store,
            workflows=[render_video],
            steps=STEP_DEFS,
            worker_options=WorkerOptions(
                task_queues=(GPU_QUEUE,),
                workflow_concurrency=0,
                queue_limits={GPU_QUEUE: QueueLimit(concurrency=2)},
                shutdown_grace=0.5,
            ),
        )
        async with general, gpu:
            await general.execute(render_video, "vid-2", 6, timeout=10)

    assert peak_concurrency(GPU_QUEUE) == 2


async def test_gpu_queue_rate_limit_spaces_out_attempts(tmp_path: Path) -> None:
    async with SQLiteStore(tmp_path / "tq_rate.db") as store:
        general = Loomwork(
            store=store,
            workflows=[render_video],
            steps=STEP_DEFS,
            worker_options=WorkerOptions(task_queues=("default",), shutdown_grace=0.5),
        )
        gpu = Loomwork(
            store=store,
            workflows=[render_video],
            steps=STEP_DEFS,
            worker_options=WorkerOptions(
                task_queues=(GPU_QUEUE,),
                workflow_concurrency=0,
                queue_limits={GPU_QUEUE: QueueLimit(rate=20, burst=1)},
                shutdown_grace=0.5,
            ),
        )
        async with general, gpu:
            await general.execute(render_video, "vid-rate", 4, timeout=10)

    starts = sorted(start for queue, _tag, start, _end in timeline if queue == GPU_QUEUE)
    # Four attempts at 20/s with no burst need at least three 50ms gaps between them.
    assert starts[-1] - starts[0] >= 0.13


async def test_without_a_gpu_worker_steps_are_scheduled_but_never_claimed(
    tmp_path: Path,
) -> None:
    async with SQLiteStore(tmp_path / "tq_stuck.db") as store:
        general = Loomwork(
            store=store,
            workflows=[render_video],
            steps=STEP_DEFS,
            worker_options=WorkerOptions(task_queues=("default",), shutdown_grace=0.2),
        )
        async with general:
            handle = await general.start_workflow(render_video, "vid-3", 1)
            for _ in range(10):
                jobs = await handle.pending_steps()
                if any(job.queue == GPU_QUEUE for job in jobs):
                    break
                await asyncio.sleep(0.02)
            (job,) = await handle.pending_steps()
            assert job.queue == GPU_QUEUE
            assert job.attempt == 0  # nobody is polling the gpu queue
            await handle.terminate("test done")
