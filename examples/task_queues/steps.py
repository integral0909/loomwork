"""Steps for the video rendering pipeline.

``render_frame`` is routed to the ``"gpu"`` task queue: only a worker that subscribes
to that queue ever claims it, which is how a fleet routes work that needs specific
hardware (or credentials, or software) to the machines that actually have it.
"""

from __future__ import annotations

import asyncio
import time

import loomwork as lw

GPU_QUEUE = "gpu"

#: (queue, tag, start, end) for every step attempt actually executed -- lets the demo
#: (and its tests) verify which worker ran what, and how much rendering overlapped.
timeline: list[tuple[str, str, float, float]] = []


@lw.step(name="task_queues.extract_metadata")
async def extract_metadata(video_id: str) -> str:
    """A cheap, CPU-only step: runs on the default queue."""
    started = time.monotonic()
    await asyncio.sleep(0.01)
    timeline.append(("default", f"metadata:{video_id}", started, time.monotonic()))
    return "1080p"


@lw.step(name="task_queues.render_frame", queue=GPU_QUEUE)
async def render_frame(video_id: str, frame: int) -> str:
    """A GPU-heavy step: only a worker subscribed to the ``"gpu"`` queue runs this."""
    tag = f"{video_id}:frame-{frame}"
    started = time.monotonic()
    await asyncio.sleep(0.1)
    timeline.append((GPU_QUEUE, tag, started, time.monotonic()))
    return tag


@lw.step(name="task_queues.finalize_video")
async def finalize_video(video_id: str, resolution: str, frame_tags: list[str]) -> str:
    """Stitch the rendered frames together: back on the default queue."""
    started = time.monotonic()
    await asyncio.sleep(0.01)
    timeline.append(("default", f"final:{video_id}", started, time.monotonic()))
    return f"{video_id}.{resolution}.mp4 ({len(frame_tags)} frames)"


def peak_concurrency(queue: str) -> int:
    """The most attempts on ``queue`` that were ever running at the same time."""
    events: list[tuple[float, int]] = []
    for q, _tag, start, end in timeline:
        if q != queue:
            continue
        events.append((start, 1))
        events.append((end, -1))
    events.sort()
    running = peak = 0
    for _time_point, delta in events:
        running += delta
        peak = max(peak, running)
    return peak
