"""The render pipeline: metadata, then frames on the GPU queue, then a final mux."""

from __future__ import annotations

import asyncio

import loomwork as lw
from examples.task_queues.steps import extract_metadata, finalize_video, render_frame


@lw.workflow(name="task_queues.render_video")
async def render_video(video_id: str, frame_count: int) -> str:
    resolution = await extract_metadata(video_id)
    frame_tags = list(
        await asyncio.gather(*(render_frame(video_id, frame) for frame in range(frame_count)))
    )
    return await finalize_video(video_id, resolution, frame_tags)
