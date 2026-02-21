"""Client API for schedules."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from loomwork.codec.converter import DataConverter
from loomwork.options import IdConflictPolicy, IdReusePolicy, VersioningPolicy, as_timedelta
from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    validate_schedule_id,
)
from loomwork.schedules.service import ScheduleService
from loomwork.sdk.definitions import WorkflowLike, as_definition
from loomwork.visibility.attributes import encode_attributes


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
        action: ScheduleAction | None = None,
        note: str | None = None,
    ) -> ScheduleState:
        """Change fields of the schedule; only the arguments given are changed.

        ``action`` replaces the whole action, including its arguments and every id,
        versioning and timeout policy -- construct it with ``dataclasses.replace`` on
        ``(await handle.describe()).action`` to change just one field of it.
        """
        changes: dict[str, Any] = {}
        if spec is not None:
            changes["spec"] = spec
        if policy is not None:
            changes["policy"] = policy
        if action is not None:
            changes["action"] = action
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
        workflow: WorkflowLike | str,
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
        id_reuse_policy: IdReusePolicy | str | None = None,
        id_conflict_policy: IdConflictPolicy | str | None = None,
        versioning_policy: VersioningPolicy | str | None = None,
        run_timeout: timedelta | float | None = None,
        execution_timeout: timedelta | float | None = None,
        search_attributes: dict[str, Any] | None = None,
    ) -> ScheduleHandle:
        """Create a schedule that starts ``workflow(*args)`` on ``cron`` or ``interval``.

        ``id_reuse_policy``, ``id_conflict_policy`` and ``versioning_policy`` govern
        every run the schedule starts, exactly as the same-named arguments do for
        :func:`loomwork.start_child` -- unset, each reproduces today's behaviour (see
        ``docs/guides/schedules.md`` for how ``id_conflict_policy`` interacts with
        ``overlap``). ``run_timeout`` and ``execution_timeout`` bound each run the same
        way as :meth:`~loomwork.engine.engine.Engine.start_workflow`'s arguments of the
        same name. ``search_attributes`` is validated and encoded once, here.

        Raises:
            ScheduleExistsError: a schedule with this id exists.
        """
        if isinstance(workflow, str):
            name, positional = workflow, list(args)
        else:
            definition = as_definition(workflow)
            name, positional = definition.name, definition.bind(args)
        config = self._service.config
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
            action=ScheduleAction(
                name,
                await self._converter.encode_args_async(positional),
                id_reuse_policy=IdReusePolicy(id_reuse_policy) if id_reuse_policy else None,
                id_conflict_policy=(
                    IdConflictPolicy(id_conflict_policy) if id_conflict_policy else None
                ),
                versioning_policy=(
                    VersioningPolicy(versioning_policy) if versioning_policy else None
                ),
                run_timeout=as_timedelta(run_timeout),
                execution_timeout=as_timedelta(execution_timeout),
                search_attributes=(
                    encode_attributes(
                        search_attributes, config.search_attributes if config else None
                    )
                    if search_attributes
                    else None
                ),
            ),
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
