"""A media-rendering job: render an image, then a report referencing it.

Nothing here is aware of offloading, compression or encryption -- the workflow just
passes a `bytes` result from one step into the next. Whether that costs a few bytes of
history or several megabytes is entirely a property of the codec chain the caller
configures the ``DataConverter`` with (see ``main.py``).
"""

from __future__ import annotations

import loomwork as lw
from examples.media_processing.models import MediaJobResult
from examples.media_processing.steps import render_image, render_report


@lw.workflow(name="media_processing.render_job")
async def render_job(job_id: str, width: int, height: int) -> MediaJobResult:
    image = await render_image(job_id, width, height)
    report = await render_report(job_id, image)
    return MediaJobResult(job_id=job_id, image_bytes=len(image), report_bytes=len(report))
