"""Steps that produce large, binary outputs: a rendered image, then a report built
from it -- so a multi-megabyte value flows from step to step, exactly the scenario
``OffloadCodec`` (see ``docs/large-payloads.md``) exists for.

The content is a repeating, recognizable marker rather than random bytes, so ``main.py``
can prove (from outside the workflow entirely) that the marker survives a normal
decode but never appears on disk in the blob store: what actually reaches storage is
compressed, encrypted bytes, not the plaintext image or report.
"""

from __future__ import annotations

import os

import loomwork as lw

#: Embedded once at the front of every generated image and report. Used only to prove
#: the plaintext never reaches the blob store; a real image or report would have no
#: such marker.
PLAINTEXT_MARKER = b"MEDIA-PROCESSING-PLAINTEXT-MARKER-"


def _filler(size: int) -> bytes:
    """``size`` bytes: :data:`PLAINTEXT_MARKER` followed by random padding.

    The padding is random, not repeated, so the payload does not compress away to
    nothing before it reaches the offload threshold -- a real rendered image or report
    is already-compressed data (JPEG, a zipped bundle, ...), not a repeating pattern.
    """
    return PLAINTEXT_MARKER + os.urandom(max(size - len(PLAINTEXT_MARKER), 0))


@lw.step(name="media_processing.render_image")
async def render_image(_job_id: str, width: int, height: int) -> bytes:
    """Render a ``width`` x ``height`` image, three (synthetic) bytes per pixel."""
    return _filler(width * height * 3)


@lw.step(name="media_processing.render_report")
async def render_report(job_id: str, image: bytes) -> bytes:
    """Build a report referencing the rendered image.

    Takes the rendered image itself as an argument (not just its size), so a large
    value flows *into* this step too, not only out of :func:`render_image` -- both the
    argument and the result go through the codec chain independently.
    """
    header = f"report for {job_id}: image is {len(image)} bytes\n".encode()
    return header + _filler(len(image) // 2)
