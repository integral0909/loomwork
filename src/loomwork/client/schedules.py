"""Client API for schedules."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from loomwork.codec.converter import DataConverter
from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    validate_schedule_id,
)
from loomwork.schedules.service import ScheduleService
from loomwork.sdk.definitions import WorkflowDefinition


class ScheduleHandle:
    """Refers to one schedule."""

    def __init__(self, service: ScheduleService, schedule_id: str) -> None:
        self._service = service
        self.schedule_id = schedule_id

    def __repr__(self) -> str:
        return f"ScheduleHandle({self.schedule_id!r})"

    async def describe(self) -> ScheduleState:
        return await self._service.store.get_schedule(self.schedule_id)

    async def pause(self, note: str = "") -> ScheduleState:
        return await self._service.update(self.schedule_id, paused=True, note=note)

    async def unpause(self, note: str = "") -> ScheduleState:
        return await self._service.update(self.schedule_id, paused=False, note=note)

    async def trigger(self) -> ScheduleState:
        """Start one run immediately."""
        return await self._service.trigger(self.schedule_id)

    async def backfill(self, start: datetime, end: datetime) -> ScheduleState:
        """Start runs for every fire time in ``(start, end]``."""
        return await self._service.backfill(self.schedule_id, start, end)

    async def update(
        self,
        *,
        spec: ScheduleSpec | None = None,
        policy: SchedulePolicy | None = None,
        note: str | None = None,
    ) -> ScheduleState:
        changes: dict[str, Any] = {}
        if spec is not None:
            changes["spec"] = spec
        if policy is not None:
            changes["policy"] = policy
        if note is not None:
            changes["note"] = note
        return await self._service.update(self.schedule_id, **changes)

    async def upcoming(self, count: int = 5) -> Sequence[datetime]:
        return await self._service.upcoming(self.schedule_id, count)

    async def delete(self) -> bool:
        return await self._service.store.delete_schedule(self.schedule_id)


class ScheduleClient:
    """Creates and finds schedules. Available as ``client.schedules``."""

    def __init__(self, service: ScheduleService, converter: DataConverter) -> None:
        self._service = service
        self._converter = converter

    async def create(
        self,
        schedule_id: str,
        workflow: WorkflowDefinition[..., Any] | str,
        *args: Any,
        cron: str | None = None,
        interval: timedelta | float | None = None,
        timezone: str = "UTC",
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        jitter: timedelta | float = 0,
        overlap: OverlapPolicy | str = OverlapPolicy.SKIP,
        catchup_window: timedelta | float = timedelta(minutes=1),
        pause_on_failure: bool = False,
        paused: bool = False,
        note: str = "",
    ) -> ScheduleHandle:
        """Create a schedule that starts ``workflow(*args)`` on ``cron`` or ``interval``.

        Raises:
            ScheduleExistsError: a schedule with this id exists.
        """
        if isinstance(workflow, WorkflowDefinition):
            name, positional = workflow.name, workflow.bind(args)
        else:
            name, positional = workflow, list(args)
        state = ScheduleState(
            schedule_id=validate_schedule_id(schedule_id),
            spec=ScheduleSpec(
                cron=cron,
                interval=interval,
                timezone=timezone,
                start_at=start_at,
                end_at=end_at,
                jitter=jitter,
            ),
            action=ScheduleAction(name, self._converter.encode_args(positional)),
            policy=SchedulePolicy(overlap, catchup_window, pause_on_failure),
            paused=paused,
            note=note,
        )
        await self._service.create(state)
        return ScheduleHandle(self._service, schedule_id)

    def get(self, schedule_id: str) -> ScheduleHandle:
        return ScheduleHandle(self._service, schedule_id)

    async def list(self, limit: int = 100) -> list[ScheduleState]:
        return await self._service.store.list_schedules(limit)
