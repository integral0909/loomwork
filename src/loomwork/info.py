"""Read-only information about the currently executing workflow or step."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class WorkflowInfo:
    """Identity of a workflow run, available inside workflow code via ``loomwork.info()``."""

    workflow_id: str
    run_id: str
    workflow_name: str
    attempt: int
    started_at: datetime


@dataclass(frozen=True, slots=True)
class StepInfo:
    """Identity of a step attempt, available inside steps via ``loomwork.step_info()``.

    ``idempotency_key`` is stable across retries of the same scheduled step and is the
    recommended key for downstream APIs that support idempotent requests.
    """

    workflow_id: str
    run_id: str
    workflow_name: str
    step_name: str
    seq: int
    attempt: int
    scheduled_at: datetime

    @property
    def idempotency_key(self) -> str:
        return f"{self.run_id}:{self.seq}"
