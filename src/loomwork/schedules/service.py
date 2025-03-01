"""Evaluating schedules and applying their decisions.

:class:`ScheduleService` turns the pure plans of :mod:`loomwork.schedules.planner` into
store operations. It is used by workers (the scheduler loop) and by clients (creating,
pausing, triggering and backfilling schedules).

Concurrency: several workers may evaluate the same schedule at once. Every write is
checked against the version that was read, so exactly one evaluation wins; the others
see :class:`ScheduleConflictError` and move on.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Sequence
from datetime import datetime

from loomwork.clock import Clock
from loomwork.errors import NotFoundError, ScheduleConflictError
from loomwork.history.events import WorkflowStarted
from loomwork.ids import new_id
from loomwork.schedules.model import ScheduledRun, ScheduleState
from loomwork.schedules.planner import (
    SchedulePlan,
    plan,
    plan_backfill,
    plan_trigger,
    record_starts,
)
from loomwork.store.base import Store
from loomwork.store.records import (
    ChildSpec,
    ExecutionRecord,
    ExecutionStatus,
    OutboxAction,
    OutboxMessage,
)

logger = logging.getLogger("loomwork.schedules")


class ScheduleService:
    def __init__(self, store: Store, clock: Clock) -> None:
        self.store = store
        self.clock = clock
        self.evaluations = 0
        self.runs_started = 0

    # ------------------------------------------------------------------ evaluation

    async def run_once(self, limit: int = 100) -> int:
        """Evaluate every due schedule once. Returns how many were evaluated."""
        now = self.clock.now()
        due = await self.store.due_schedules(now, limit)
        for state in due:
            try:
                await self.evaluate(state, now)
            except ScheduleConflictError:
                logger.debug("schedule %s changed concurrently; skipping", state.schedule_id)
            except Exception:
                logger.exception("failed to evaluate schedule %s", state.schedule_id)
        return len(due)

    async def _statuses(self, state: ScheduleState) -> dict[str, ExecutionStatus]:
        statuses: dict[str, ExecutionStatus] = {}
        for run in state.running:
            try:
                statuses[run.run_id] = (await self.store.get_execution(run.run_id)).status
            except NotFoundError:
                continue
        return statuses

    async def evaluate(self, state: ScheduleState, now: datetime) -> ScheduleState:
        result = plan(state, now, await self._statuses(state))
        if result.paused_now:
            logger.warning("schedule %s paused after a run failed", state.schedule_id)
        if result.skipped:
            logger.info("schedule %s skipped %d fire time(s)", state.schedule_id, result.skipped)
        return await self.apply(result, state.version, now)

    async def apply(
        self, result: SchedulePlan, expected_version: int, now: datetime
    ) -> ScheduleState:
        state = result.state
        specs: list[ChildSpec] = []
        started: list[ScheduledRun] = []
        for start in result.starts:
            run_id = new_id()
            record = ExecutionRecord(
                run_id=run_id,
                workflow_id=start.workflow_id,
                workflow_name=state.action.workflow_name,
                status=ExecutionStatus.RUNNING,
                created_at=now,
            )
            event = WorkflowStarted(
                workflow_name=state.action.workflow_name,
                workflow_id=start.workflow_id,
                run_id=run_id,
                args=list(state.action.args),
                random_seed=secrets.randbits(63),
                schedule_id=state.schedule_id,
                scheduled_time=start.nominal_time,
            )
            specs.append(ChildSpec(record=record, started=event))
            started.append(ScheduledRun(start.nominal_time, now, start.workflow_id, run_id))
        outbox: list[OutboxMessage] = [
            OutboxMessage(
                OutboxAction.REQUEST_CANCEL,
                target_run_id=run_id,
                reason=f"schedule {state.schedule_id} started a newer run",
            )
            for run_id in result.cancel_run_ids
        ] + [
            OutboxMessage(
                OutboxAction.TERMINATE,
                target_run_id=run_id,
                reason=f"schedule {state.schedule_id} started a newer run",
            )
            for run_id in result.terminate_run_ids
        ]
        state = record_starts(state, started, now) if started else state.with_(updated_at=now)
        saved = await self.store.fire_schedule(state, expected_version, specs, outbox, now)
        self.evaluations += 1
        started_ids = {run.run_id for run in started}
        self.runs_started += sum(1 for run in saved.running if run.run_id in started_ids)
        return saved

    # ------------------------------------------------------------------ management

    async def create(self, state: ScheduleState) -> ScheduleState:
        now = self.clock.now()
        initial = state.with_(last_evaluated=now, next_fire_at=_first_fire(state, now), version=0)
        return await self.store.create_schedule(initial, now)

    async def update(self, schedule_id: str, **changes: object) -> ScheduleState:
        """Change fields of a schedule (retrying on concurrent evaluations)."""
        for _ in range(5):
            current = await self.store.get_schedule(schedule_id)
            now = self.clock.now()
            updated = current.with_(**changes)
            if "spec" in changes or "paused" in changes:
                # Resuming or rescheduling never backfills the time in between.
                updated = updated.with_(
                    last_evaluated=now, next_fire_at=_first_fire(updated, now), buffered=None
                )
            try:
                return await self.store.update_schedule(updated, current.version, now)
            except ScheduleConflictError:
                continue
        raise ScheduleConflictError(f"schedule {schedule_id!r} keeps changing; try again")

    async def trigger(self, schedule_id: str) -> ScheduleState:
        """Start one run now, regardless of the spec and overlap policy."""
        state = await self.store.get_schedule(schedule_id)
        now = self.clock.now()
        return await self.apply(plan_trigger(state, now), state.version, now)

    async def backfill(self, schedule_id: str, start: datetime, end: datetime) -> ScheduleState:
        """Start a run for every nominal fire time in ``(start, end]``."""
        state = await self.store.get_schedule(schedule_id)
        now = self.clock.now()
        return await self.apply(plan_backfill(state, start, end), state.version, now)

    async def upcoming(self, schedule_id: str, count: int = 5) -> Sequence[datetime]:
        state = await self.store.get_schedule(schedule_id)
        times: list[datetime] = []
        current = self.clock.now()
        while len(times) < count:
            nxt = state.spec.next_after(current)
            if nxt is None:
                break
            times.append(nxt)
            current = nxt
        return times


def _first_fire(state: ScheduleState, now: datetime) -> datetime | None:
    nominal = state.spec.next_after(now)
    if nominal is None:
        return None
    return nominal + state.spec.jitter_for(state.schedule_id, nominal)
