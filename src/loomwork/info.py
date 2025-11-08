"""Read-only information about the currently executing workflow or step."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class WorkflowInfo:
    """Identity of a workflow run, available inside workflow code via ``loomwork.info()``."""

    workflow_id: str
    run_id: str
    workflow_name: str
    attempt: int
    started_at: datetime
    parent_workflow_id: str | None = None
    parent_run_id: str | None = None
    schedule_id: str | None = None
    scheduled_time: datetime | None = None
    """The nominal fire time of the schedule that started this run (for example the
    date a daily report is about), independent of when the run actually started."""
    continued_from_run_id: str | None = None
    """The run that continued as new into this one, if any."""
    headers: dict[str, str] = field(default_factory=dict)
    """Context propagated to this run (for example a ``traceparent``), as recorded on
    its ``WorkflowStarted``. See :func:`loomwork.current_trace_context`."""


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
    headers: dict[str, str] = field(default_factory=dict)
    """Context propagated from the workflow that scheduled this step (see
    :func:`loomwork.current_trace_context`)."""

    @property
    def idempotency_key(self) -> str:
        return f"{self.run_id}:{self.seq}"
