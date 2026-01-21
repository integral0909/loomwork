"""The workflow's result: sizes only, never the rendered bytes themselves -- the run's
final result stays small even though its steps' inputs and outputs do not.
"""

from __future__ import annotations

from pydantic import BaseModel


class MediaJobResult(BaseModel):
    job_id: str
    image_bytes: int
    report_bytes: int
