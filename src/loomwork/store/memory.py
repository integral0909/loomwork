"""In-memory store for tests and embedded use without durability.

Every public method runs without awaiting, so on a single event loop each call is atomic
without locks. The store is not thread-safe and holds everything in process memory: a
restart loses all runs. Use the SQLite or PostgreSQL store for durability.
"""

from __future__ import annotations

import copy
import dataclasses
import heapq
from collections.abc import Collection
from datetime import datetime, timedelta

from loomwork.errors import LeaseLostError, NotFoundError, WorkflowAlreadyStartedError
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    CancelRequested,
    Event,
    StepCompleted,
    StepFailed,
    TickCompleted,
    TimerFired,
    WorkflowStarted,
    WorkflowTerminated,
)
from loomwork.store.base import Store
from loomwork.store.notify import TOPIC_STEPS, TOPIC_TICKS, TOPIC_TIMERS, closed_topic
from loomwork.store.records import (
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    StepJob,
    TickOutcome,
    TimerRecord,
)


@dataclasses.dataclass(slots=True)
class _Run:
    record: ExecutionRecord
    events: list[Event] = dataclasses.field(default_factory=list)
    step_jobs: dict[int, str] = dataclasses.field(default_factory=dict)  # seq -> job id
    timers: set[int] = dataclasses.field(default_factory=set)


class MemoryStore(Store):
    """A complete, non-durable :class:`Store` implementation."""

    def __init__(self) -> None:
        super().__init__()
        self._runs: dict[str, _Run] = {}
        self._by_workflow_id: dict[str, list[str]] = {}
        self._order: list[str] = []
        self._tick_candidates: set[str] = set()
        self._jobs: dict[str, StepJob] = {}
        self._timers: dict[tuple[str, int], TimerRecord] = {}
        self._timer_heap: list[tuple[datetime, str, int]] = []

    # ------------------------------------------------------------------ helpers

    def _run(self, run_id: str) -> _Run:
        try:
            return self._runs[run_id]
        except KeyError:
            raise NotFoundError(f"workflow run {run_id!r} does not exist") from None

    @staticmethod
    def _append(run: _Run, event: Event, now: datetime) -> Event:
        stored = dataclasses.replace(event, event_id=run.record.last_event_id + 1, timestamp=now)
        run.events.append(stored)
        run.record.last_event_id = stored.event_id
        if stored.is_input:
            run.record.last_input_id = stored.event_id
        return stored

    def _mark_tick_pending(self, run: _Run, now: datetime) -> None:
        record = run.record
        if record.is_closed:
            return
        if record.last_input_id > record.consumed_through:
            if not record.tick_pending:
                record.tick_pending = True
                record.tick_ready_at = now
            self._tick_candidates.add(record.run_id)
            self.notifier.publish(TOPIC_TICKS)
        else:
            record.tick_pending = False
            record.tick_ready_at = None
            self._tick_candidates.discard(record.run_id)

    def _close(self, run: _Run, outcome: CloseOutcome, now: datetime) -> None:
        record = run.record
        record.status = outcome.status
        record.closed_at = now
        record.result = outcome.result
        record.failure = outcome.failure
        record.close_reason = outcome.reason
        record.tick_pending = False
        record.tick_ready_at = None
        record.lease_owner = None
        record.lease_expires_at = None
        self._tick_candidates.discard(record.run_id)
        for job_id in run.step_jobs.values():
            self._jobs.pop(job_id, None)
        run.step_jobs.clear()
        for seq in run.timers:
            self._timers.pop((record.run_id, seq), None)
        run.timers.clear()
        self.notifier.publish(closed_topic(record.run_id))

    def _add_timer(self, run: _Run, timer: TimerRecord) -> None:
        self._timers[(timer.run_id, timer.seq)] = timer
        run.timers.add(timer.seq)
        heapq.heappush(self._timer_heap, (timer.fire_at, timer.run_id, timer.seq))

    def _remove_timer(self, run: _Run, seq: int) -> None:
        self._timers.pop((run.record.run_id, seq), None)
        run.timers.discard(seq)

    @staticmethod
    def _lease_free(owner: str | None, expires: datetime | None, now: datetime) -> bool:
        return owner is None or (expires is not None and expires <= now)

    # ------------------------------------------------------------------ executions

    async def create_execution(
        self, record: ExecutionRecord, started: WorkflowStarted, now: datetime
    ) -> tuple[ExecutionRecord, Event]:
        for run_id in self._by_workflow_id.get(record.workflow_id, ()):
            existing = self._runs[run_id].record
            if not existing.is_closed:
                raise WorkflowAlreadyStartedError(record.workflow_id, existing.run_id)
        if record.run_id in self._runs:
            raise WorkflowAlreadyStartedError(record.workflow_id, record.run_id)
        run = _Run(record=copy.copy(record))
        self._runs[record.run_id] = run
        self._by_workflow_id.setdefault(record.workflow_id, []).append(record.run_id)
        self._order.append(record.run_id)
        event = self._append(run, started, now)
        self._mark_tick_pending(run, now)
        return copy.copy(run.record), event

    async def get_execution(self, run_id: str) -> ExecutionRecord:
        return copy.copy(self._run(run_id).record)

    async def find_execution(self, workflow_id: str) -> ExecutionRecord:
        run_ids = self._by_workflow_id.get(workflow_id)
        if not run_ids:
            raise NotFoundError(f"no run exists for workflow id {workflow_id!r}")
        return copy.copy(self._runs[run_ids[-1]].record)

    async def list_executions(
        self,
        *,
        workflow_name: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        out: list[ExecutionRecord] = []
        for run_id in reversed(self._order):
            record = self._runs[run_id].record
            if workflow_name is not None and record.workflow_name != workflow_name:
                continue
            if status is not None and record.status is not status:
                continue
            out.append(copy.copy(record))
            if len(out) >= limit:
                break
        return out

    # ------------------------------------------------------------------ history

    async def load_history(
        self, run_id: str, after_id: int = 0, limit: int | None = None
    ) -> list[Event]:
        events = self._run(run_id).events
        # Event ids are dense and 1-based, so the id is the list index + 1.
        selected = events[max(after_id, 0) :]
        return selected if limit is None else selected[:limit]

    async def append_input(self, run_id: str, event: Event, now: datetime) -> Event | None:
        if not event.is_input:
            raise ValueError(f"{type(event).__name__} is not an input event")
        run = self._run(run_id)
        if run.record.is_closed:
            return None
        stored = self._append(run, event, now)
        self._mark_tick_pending(run, now)
        return stored

    async def request_cancel(self, run_id: str, reason: str, now: datetime) -> bool:
        run = self._run(run_id)
        if run.record.is_closed:
            return False
        if not run.record.cancel_requested:
            run.record.cancel_requested = True
            self._append(run, CancelRequested(reason=reason), now)
            self._mark_tick_pending(run, now)
        return True

    async def terminate(self, run_id: str, reason: str, now: datetime) -> bool:
        run = self._run(run_id)
        if run.record.is_closed:
            return False
        self._append(run, WorkflowTerminated(reason=reason), now)
        self._close(run, CloseOutcome(ExecutionStatus.TERMINATED, reason=reason), now)
        return True

    # ------------------------------------------------------------------ ticks

    async def claim_tick(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        workflow_names: Collection[str] | None = None,
    ) -> ExecutionRecord | None:
        best: ExecutionRecord | None = None
        for run_id in self._tick_candidates:
            record = self._runs[run_id].record
            if record.tick_ready_at is None or record.tick_ready_at > now:
                continue
            if not self._lease_free(record.lease_owner, record.lease_expires_at, now):
                continue
            if workflow_names is not None and record.workflow_name not in workflow_names:
                continue
            if best is None or (record.tick_ready_at, record.run_id) < (
                best.tick_ready_at or now,
                best.run_id,
            ):
                best = record
        if best is None:
            return None
        best.lease_owner = worker_id
        best.lease_expires_at = now + lease
        return copy.copy(best)

    async def complete_tick(
        self, run_id: str, worker_id: str, outcome: TickOutcome, now: datetime
    ) -> list[Event]:
        run = self._run(run_id)
        record = run.record
        if record.lease_owner != worker_id:
            raise LeaseLostError(f"worker {worker_id} does not hold the lease on run {run_id}")
        if record.is_closed:
            raise LeaseLostError(f"run {run_id} was closed while the tick was running")

        appended: list[Event] = []
        consumed_new_input = outcome.consumed_through > record.consumed_through
        if consumed_new_input or outcome.decisions:
            tick = TickCompleted(
                consumed_through=max(outcome.consumed_through, record.consumed_through),
                time=outcome.time,
                worker_id=worker_id,
            )
            appended.append(self._append(run, tick, now))
            appended.extend(self._append(run, decision, now) for decision in outcome.decisions)
        record.consumed_through = max(record.consumed_through, outcome.consumed_through)
        record.lease_owner = None
        record.lease_expires_at = None
        record.tick_failures = 0
        record.last_tick_error = None

        if outcome.close is not None:
            self._close(run, outcome.close, now)
            return appended

        for seq in outcome.cancel_steps:
            job_id = run.step_jobs.get(seq)
            job = self._jobs.get(job_id) if job_id else None
            if job is None:
                continue
            if self._lease_free(job.lease_owner, job.lease_expires_at, now):
                del self._jobs[job.job_id]
                del run.step_jobs[seq]
            else:
                job.cancel_requested = True
        for job in outcome.new_steps:
            stored_job = copy.copy(job)
            self._jobs[job.job_id] = stored_job
            run.step_jobs[job.seq] = job.job_id
        for seq in outcome.cancel_timers:
            self._remove_timer(run, seq)
        for timer in outcome.new_timers:
            self._add_timer(run, timer)

        if outcome.new_steps:
            self.notifier.publish(TOPIC_STEPS)
        if outcome.new_timers or outcome.cancel_timers:
            self.notifier.publish(TOPIC_TIMERS)
        record.tick_pending = False
        self._mark_tick_pending(run, now)
        return appended

    async def fail_tick(
        self,
        run_id: str,
        worker_id: str,
        error: str,
        retry_at: datetime,
        now: datetime,
    ) -> None:
        run = self._run(run_id)
        record = run.record
        if record.lease_owner != worker_id:
            raise LeaseLostError(f"worker {worker_id} does not hold the lease on run {run_id}")
        record.lease_owner = None
        record.lease_expires_at = None
        record.tick_failures += 1
        record.last_tick_error = error
        if not record.is_closed:
            record.tick_pending = True
            record.tick_ready_at = retry_at
            self._tick_candidates.add(run_id)
            self.notifier.publish(TOPIC_TICKS)

    # ------------------------------------------------------------------ steps

    async def claim_step(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        step_names: Collection[str] | None = None,
    ) -> StepJob | None:
        best: StepJob | None = None
        abandoned: list[StepJob] = []
        for job in self._jobs.values():
            if not self._lease_free(job.lease_owner, job.lease_expires_at, now):
                continue
            if job.cancel_requested:
                abandoned.append(job)
                continue
            if job.ready_at > now:
                continue
            if step_names is not None and job.step_name not in step_names:
                continue
            if best is None or (job.ready_at, job.job_id) < (best.ready_at, best.job_id):
                best = job
        for job in abandoned:
            # Cancelled while an attempt was running and that attempt never reported
            # back: nobody is interested in the outcome any more.
            self._delete_job(job)
        if best is None:
            return None
        best.attempt += 1
        best.lease_owner = worker_id
        best.lease_expires_at = now + lease
        return copy.copy(best)

    def _delete_job(self, job: StepJob) -> None:
        self._jobs.pop(job.job_id, None)
        run = self._runs.get(job.run_id)
        if run is not None and run.step_jobs.get(job.seq) == job.job_id:
            del run.step_jobs[job.seq]

    def _owned_job(self, job_id: str, worker_id: str) -> StepJob | None:
        job = self._jobs.get(job_id)
        if job is None or job.lease_owner != worker_id:
            return None
        return job

    async def finish_step(
        self,
        job_id: str,
        worker_id: str,
        event: StepCompleted | StepFailed,
        now: datetime,
    ) -> bool:
        job = self._owned_job(job_id, worker_id)
        if job is None:
            return False
        del self._jobs[job_id]
        run = self._runs[job.run_id]
        run.step_jobs.pop(job.seq, None)
        if not run.record.is_closed:
            self._append(run, event, now)
            self._mark_tick_pending(run, now)
        return True

    async def retry_step(
        self,
        job_id: str,
        worker_id: str,
        ready_at: datetime,
        failure: FailureInfo,
        now: datetime,
    ) -> bool:
        job = self._owned_job(job_id, worker_id)
        if job is None:
            return False
        if job.cancel_requested:
            self._delete_job(job)
            return True
        job.lease_owner = None
        job.lease_expires_at = None
        job.ready_at = ready_at
        job.last_failure = failure
        self.notifier.publish(TOPIC_STEPS)
        return True

    async def extend_step_lease(self, job_id: str, worker_id: str, expires_at: datetime) -> bool:
        job = self._owned_job(job_id, worker_id)
        if job is None:
            return False
        if job.lease_expires_at is None or expires_at > job.lease_expires_at:
            job.lease_expires_at = expires_at
        return True

    async def get_step_jobs(self, run_id: str) -> list[StepJob]:
        run = self._run(run_id)
        return [copy.copy(self._jobs[job_id]) for _, job_id in sorted(run.step_jobs.items())]

    # ------------------------------------------------------------------ timers

    def _prune_timer_heap(self) -> None:
        heap = self._timer_heap
        while heap:
            fire_at, run_id, seq = heap[0]
            timer = self._timers.get((run_id, seq))
            if timer is not None and timer.fire_at == fire_at:
                return
            heapq.heappop(heap)

    async def next_timer_at(self) -> datetime | None:
        self._prune_timer_heap()
        return self._timer_heap[0][0] if self._timer_heap else None

    async def fire_due_timers(self, now: datetime, limit: int = 100) -> int:
        fired = 0
        while fired < limit:
            self._prune_timer_heap()
            if not self._timer_heap or self._timer_heap[0][0] > now:
                break
            _, run_id, seq = heapq.heappop(self._timer_heap)
            run = self._runs[run_id]
            self._remove_timer(run, seq)
            if not run.record.is_closed:
                self._append(run, TimerFired(seq=seq), now)
                self._mark_tick_pending(run, now)
            fired += 1
        return fired

    # ------------------------------------------------------------------ introspection

    @property
    def run_count(self) -> int:
        return len(self._runs)

    @property
    def job_count(self) -> int:
        return len(self._jobs)

    @property
    def timer_count(self) -> int:
        return len(self._timers)
