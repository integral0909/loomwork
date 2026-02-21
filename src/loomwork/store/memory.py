"""In-memory store for tests and embedded use without durability.

Every public method runs without awaiting, so on a single event loop each call is atomic
without locks. The store is not thread-safe and holds everything in process memory: a
restart loses all runs. Use the SQLite or PostgreSQL store for durability.
"""

from __future__ import annotations

import copy
import dataclasses
import heapq
import json
from collections import deque
from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from loomwork.codec.payload import Payload
from loomwork.errors import (
    LeaseLostError,
    NotFoundError,
    ScheduleConflictError,
    ScheduleExistsError,
    WorkflowAlreadyStartedError,
    WorkflowClosedError,
    WorkflowIdReuseRejectedError,
)
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
    WorkflowTimedOut,
)
from loomwork.history.serde import event_to_dict
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint, VersioningPolicy
from loomwork.schedules.model import ScheduleState
from loomwork.visibility.attributes import decode_attributes, merge

if TYPE_CHECKING:
    from loomwork.visibility.schema import SearchAttributes
from loomwork.store.base import Store, id_reuse_allowed
from loomwork.store.closing import (
    DEADLINE_TIMER_SEQ,
    attach_unstarted,
    child_id_reuse_rejected_event,
    child_outcome_event_for,
    child_start_failed_event,
    closing_messages,
    drop_unstarted,
    reconcile_running,
    run_deadline,
)
from loomwork.store.notify import (
    TOPIC_OUTBOX,
    TOPIC_SCHEDULES,
    TOPIC_STEPS,
    TOPIC_TICKS,
    TOPIC_TIMERS,
    closed_topic,
    run_topic,
)
from loomwork.store.records import (
    BuildCompatibility,
    ChildSpec,
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    HeartbeatResult,
    OutboxAction,
    OutboxMessage,
    StepJob,
    TickOutcome,
    TimerRecord,
    WorkerPresence,
)
from loomwork.store.reset import plan_reset, reconstruct_step_job, reconstruct_timer


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
        self._children: dict[str, list[str]] = {}
        self._schedules: dict[str, ScheduleState] = {}
        self._attributes: dict[str, dict[str, Any]] = {}
        self._outbox: deque[OutboxMessage] = deque()
        self._build_compat: dict[tuple[str, str], datetime] = {}
        self._worker_presence: dict[str, WorkerPresence] = {}
        #: run_id -> [(watcher_run_id, watcher_seq), ...]: extra parents attached with
        #: IdConflictPolicy.USE_EXISTING after the run was already going, reported when
        #: it (or the run it continues as, transitively) finally closes. See
        #: docs/guides/composition.md.
        self._watchers: dict[str, list[tuple[str, int]]] = {}

    def _build_eligible(self, pinned_build_id: str | None, worker_build_id: str | None) -> bool:
        """Mirrors the SQL stores' claim filter (see ``store/base.py``): an unversioned
        worker (``worker_build_id is None``) is eligible for everything, unchanged from
        before this feature; a versioned worker needs a matching or declared-compatible
        pin, or no pin at all."""
        if worker_build_id is None or pinned_build_id is None:
            return True
        if pinned_build_id == worker_build_id:
            return True
        return (worker_build_id, pinned_build_id) in self._build_compat

    # ------------------------------------------------------------------ helpers

    def _run(self, run_id: str) -> _Run:
        try:
            return self._runs[run_id]
        except KeyError:
            raise NotFoundError(f"workflow run {run_id!r} does not exist") from None

    def _append(self, run: _Run, event: Event, now: datetime) -> Event:
        stored = dataclasses.replace(event, event_id=run.record.last_event_id + 1, timestamp=now)
        run.events.append(stored)
        run.record.last_event_id = stored.event_id
        run.record.history_bytes += _event_size(stored)
        if stored.is_input:
            run.record.last_input_id = stored.event_id
        self.notifier.publish(run_topic(run.record.run_id))
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
        record.continued_run_id = outcome.continued_run_id
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
        running_children = [
            self._runs[child].record
            for child in self._children.get(record.run_id, ())
            if child in self._runs and not self._runs[child].record.is_closed
        ]
        messages = list(closing_messages(record, running_children))
        watchers = self._watchers.pop(record.run_id, [])
        if watchers:
            if record.continued_run_id is not None:
                # The run's identity (continue-as-new, a retry, or a reset) moves to its
                # successor, which will report to these watchers when it truly closes --
                # exactly how the primary parent link already behaves (see
                # closing_messages).
                self._watchers.setdefault(record.continued_run_id, []).extend(watchers)
            else:
                messages.extend(
                    OutboxMessage(
                        action=OutboxAction.APPEND_INPUT,
                        target_run_id=watcher_run_id,
                        event=child_outcome_event_for(watcher_seq, record),
                    )
                    for watcher_run_id, watcher_seq in watchers
                )
        self._enqueue(messages)
        self.notifier.publish(closed_topic(record.run_id))

    def _enqueue(self, messages: Sequence[OutboxMessage]) -> None:
        if messages:
            self._outbox.extend(messages)
            self.notifier.publish(TOPIC_OUTBOX)

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

    def _running_for(self, workflow_id: str) -> ExecutionRecord | None:
        for run_id in self._by_workflow_id.get(workflow_id, ()):
            existing = self._runs[run_id].record
            if not existing.is_closed:
                return existing
        return None

    def _insert_run(
        self,
        record: ExecutionRecord,
        started: Event,
        now: datetime,
        extra: Sequence[Event] = (),
        ready_at: datetime | None = None,
    ) -> tuple[_Run, Event]:
        run = _Run(record=copy.copy(record))
        self._runs[record.run_id] = run
        self._by_workflow_id.setdefault(record.workflow_id, []).append(record.run_id)
        self._order.append(record.run_id)
        if record.parent_run_id is not None:
            self._children.setdefault(record.parent_run_id, []).append(record.run_id)
        initial = getattr(started, "search_attributes", None)
        if initial:
            self._attributes[record.run_id] = merge({}, initial)
        event = self._append(run, started, now)
        for extra_event in extra:
            if not extra_event.is_input:
                raise ValueError(f"{type(extra_event).__name__} is not an input event")
            self._append(run, extra_event, now)
        self._mark_tick_pending(run, now)
        if ready_at is not None and ready_at > now:
            run.record.tick_ready_at = ready_at  # a retried run waits out its backoff
        deadline = run_deadline(started, now)
        if deadline is not None:
            self._add_timer(run, TimerRecord(record.run_id, DEADLINE_TIMER_SEQ, deadline))
        return run, event

    async def create_execution(
        self,
        record: ExecutionRecord,
        started: WorkflowStarted,
        now: datetime,
        extra_inputs: Sequence[Event] = (),
        *,
        id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL,
    ) -> tuple[ExecutionRecord, Event]:
        running = self._running_for(record.workflow_id)
        if running is not None:
            if id_conflict_policy is IdConflictPolicy.USE_EXISTING:
                return copy.copy(running), started
            if id_conflict_policy is IdConflictPolicy.TERMINATE_EXISTING:
                await self.terminate(running.run_id, "superseded by id conflict policy", now)
            else:
                raise WorkflowAlreadyStartedError(record.workflow_id, running.run_id)
        elif id_reuse_policy is not IdReusePolicy.ALLOW_DUPLICATE:
            run_ids = self._by_workflow_id.get(record.workflow_id)
            if run_ids:
                latest = self._runs[run_ids[-1]].record
                if not id_reuse_allowed(id_reuse_policy, latest.status):
                    raise WorkflowIdReuseRejectedError(
                        record.workflow_id, latest.run_id, id_reuse_policy.value
                    )
        if record.run_id in self._runs:
            raise WorkflowAlreadyStartedError(record.workflow_id, record.run_id)
        run, event = self._insert_run(record, started, now, extra_inputs)
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

    async def reset_run(
        self,
        run_id: str,
        new_run_id: str,
        to: ResetPoint,
        reason: str,
        now: datetime,
    ) -> tuple[ExecutionRecord, ExecutionRecord]:
        old = self._run(run_id)
        # Validate `to` against the run's history before mutating anything: ticks only
        # accumulate, so if this succeeds the second computation below (against a
        # history that may have grown further by then) is guaranteed to succeed too.
        plan_reset(list(old.events), to)

        was_running = not old.record.is_closed
        if was_running:
            self._append(old, WorkflowTerminated(reason=reason), now)
            self._close(
                old,
                CloseOutcome(
                    ExecutionStatus.TERMINATED, reason=reason, continued_run_id=new_run_id
                ),
                now,
            )
        else:
            # `old` itself cannot be racing a tick (closed runs reject everything), but
            # some other run of the same workflow id may have started since it closed.
            running = self._running_for(old.record.workflow_id)
            if running is not None:
                raise WorkflowAlreadyStartedError(old.record.workflow_id, running.run_id)
            old.record.continued_run_id = new_run_id

        # Exclude the WorkflowTerminated just appended: it is not part of the history
        # being reset, only a record of this call having closed the run.
        history = old.events[:-1] if was_running else list(old.events)
        plan = plan_reset(history, to)
        kept = [e for e in history if e.event_id <= plan.cutoff_event_id]
        started = kept[0]
        assert isinstance(started, WorkflowStarted)

        new_record = ExecutionRecord(
            run_id=new_run_id,
            workflow_id=old.record.workflow_id,
            workflow_name=old.record.workflow_name,
            status=ExecutionStatus.RUNNING,
            created_at=now,
            parent_run_id=old.record.parent_run_id,
            parent_seq=old.record.parent_seq,
            parent_close_policy=old.record.parent_close_policy,
            reset_from_run_id=run_id,
            versioning_policy=old.record.versioning_policy,
            # pinned_build_id is deliberately not carried over: a reset run starts
            # unpinned and pins to whichever build processes its first tick, exactly
            # like continue-as-new (see docs/adr/0006-worker-build-versioning.md).
        )
        new_run = _Run(record=new_record)
        self._runs[new_run_id] = new_run
        self._by_workflow_id.setdefault(new_record.workflow_id, []).append(new_run_id)
        self._order.append(new_run_id)
        if new_record.parent_run_id is not None:
            self._children.setdefault(new_record.parent_run_id, []).append(new_run_id)

        for event in kept:
            new_run.events.append(event)  # verbatim: same event_id and timestamp
            new_run.record.history_bytes += _event_size(event)
        new_run.record.last_event_id = plan.cutoff_event_id
        new_run.record.last_input_id = max((e.event_id for e in kept if e.is_input), default=0)
        new_run.record.consumed_through = plan.consumed_through

        if plan.search_attributes:
            self._attributes[new_run_id] = dict(plan.search_attributes)

        for scheduled in plan.open_steps:
            new_job = reconstruct_step_job(
                run_id=new_run_id,
                workflow_id=new_record.workflow_id,
                workflow_name=new_record.workflow_name,
                scheduled=scheduled,
                now=now,
            )
            self._jobs[new_job.job_id] = new_job
            new_run.step_jobs[new_job.seq] = new_job.job_id
        for timer_started in plan.open_timers:
            self._add_timer(new_run, reconstruct_timer(new_run_id, timer_started))

        deadline = run_deadline(started, now)
        if deadline is not None:
            self._add_timer(new_run, TimerRecord(new_run_id, DEADLINE_TIMER_SEQ, deadline))

        for event in plan.redeliver:
            self._append(new_run, event, now)
        self._mark_tick_pending(new_run, now)

        if plan.open_steps:
            self.notifier.publish(TOPIC_STEPS)
        if plan.open_timers or deadline is not None:
            self.notifier.publish(TOPIC_TIMERS)

        return copy.copy(old.record), copy.copy(new_run.record)

    # ------------------------------------------------------------------ ticks

    async def claim_tick(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        workflow_names: Collection[str] | None = None,
        build_id: str | None = None,
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
            if not self._build_eligible(record.pinned_build_id, build_id):
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
        _maybe_pin(record, outcome.build_id)
        if consumed_new_input or outcome.decisions:
            tick = TickCompleted(
                consumed_through=max(outcome.consumed_through, record.consumed_through),
                time=outcome.time,
                worker_id=worker_id,
                code_version=outcome.code_version,
                build_id=outcome.build_id or "",
            )
            appended.append(self._append(run, tick, now))
            appended.extend(self._append(run, decision, now) for decision in outcome.decisions)
        record.consumed_through = max(record.consumed_through, outcome.consumed_through)
        record.lease_owner = None
        record.lease_expires_at = None
        record.tick_failures = 0
        record.last_tick_error = None

        self._enqueue(outcome.outbox)
        for child in outcome.new_children:
            self._start_child(run, child, now)

        if outcome.search_attributes:
            self._attributes[run_id] = merge(
                self._attributes.get(run_id, {}), outcome.search_attributes
            )
        if outcome.close is not None:
            self._close(run, outcome.close, now)
            if outcome.continuation is not None:
                # The closing run released its workflow id a moment ago.
                successor = outcome.continuation
                self._insert_run(
                    successor.record,
                    successor.started,
                    now,
                    ready_at=outcome.continuation_ready_at,
                )
                inherited = merge(
                    self._attributes.get(run_id, {}),
                    getattr(successor.started, "search_attributes", {}) or {},
                )
                if inherited:
                    self._attributes[successor.record.run_id] = inherited
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

    def _start_child(self, parent: _Run, spec: ChildSpec, now: datetime) -> None:
        """Create ``spec`` as a child, or resolve a conflict against its workflow id's
        current or latest run according to ``spec.id_conflict_policy``/
        ``id_reuse_policy`` (mirroring :meth:`create_execution`'s rules). Every outcome
        other than a plain create is reported to the parent as an input event: this
        method never raises, since nothing awaits a child's start synchronously."""
        child = spec.record
        assert child.parent_seq is not None
        running = self._running_for(child.workflow_id)
        if running is not None:
            if spec.id_conflict_policy is IdConflictPolicy.USE_EXISTING:
                self._watchers.setdefault(running.run_id, []).append(
                    (parent.record.run_id, child.parent_seq)
                )
                return
            if spec.id_conflict_policy is IdConflictPolicy.TERMINATE_EXISTING:
                running_run = self._runs[running.run_id]
                reason = "superseded by child id conflict policy"
                self._append(running_run, WorkflowTerminated(reason=reason), now)
                self._close(
                    running_run, CloseOutcome(ExecutionStatus.TERMINATED, reason=reason), now
                )
            else:
                self._append(
                    parent,
                    child_start_failed_event(
                        child.parent_seq, child.workflow_name, child.workflow_id, running.run_id
                    ),
                    now,
                )
                return
        elif spec.id_reuse_policy is not IdReusePolicy.ALLOW_DUPLICATE:
            run_ids = self._by_workflow_id.get(child.workflow_id)
            if run_ids:
                latest = self._runs[run_ids[-1]].record
                if not id_reuse_allowed(spec.id_reuse_policy, latest.status):
                    self._append(
                        parent,
                        child_id_reuse_rejected_event(
                            child.parent_seq,
                            child.workflow_name,
                            child.workflow_id,
                            latest.run_id,
                            spec.id_reuse_policy.value,
                        ),
                        now,
                    )
                    return
        if child.run_id in self._runs:
            self._append(
                parent,
                child_start_failed_event(
                    child.parent_seq, child.workflow_name, child.workflow_id, child.run_id
                ),
                now,
            )
            return
        self._insert_run(child, spec.started, now)

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

    async def next_tick_ready_at(
        self, workflow_names: Collection[str] | None = None
    ) -> datetime | None:
        """Earliest ``tick_ready_at`` among unleased pending ticks.

        Not part of the :class:`~loomwork.store.base.Store` contract: it exists for
        ``loomwork.testing``, which needs to tell "every run is blocked purely on a
        future timer or backoff" apart from "a tick is ready right now" in order to
        skip a :class:`~loomwork.clock.ManualClock` forward without ever skipping past
        runnable work. Mirrors the eligibility filter in :meth:`claim_tick` (excluding
        the ``now`` cutoff, since callers compare the result against their own clock).
        """
        candidates = [
            record.tick_ready_at
            for run_id in self._tick_candidates
            if (record := self._runs[run_id].record).tick_ready_at is not None
            and record.lease_owner is None
            and (workflow_names is None or record.workflow_name in workflow_names)
        ]
        return min(candidates, default=None)

    # ------------------------------------------------------------------ steps

    async def claim_step(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        step_names: Collection[str] | None = None,
        queues: Collection[str] | None = None,
        build_id: str | None = None,
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
            if queues is not None and job.queue not in queues:
                continue
            if not self._build_eligible(job.pinned_build_id, build_id):
                continue
            if best is None or _claim_order(job) < _claim_order(best):
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

    async def renew_step_lease(self, job_id: str, worker_id: str, expires_at: datetime) -> bool:
        job = self._owned_job(job_id, worker_id)
        if job is None:
            return False
        job.lease_expires_at = expires_at
        return True

    async def heartbeat_step(
        self,
        job_id: str,
        worker_id: str,
        expires_at: datetime,
        details: Payload | None,
        now: datetime,
    ) -> HeartbeatResult | None:
        job = self._owned_job(job_id, worker_id)
        if job is None:
            return None
        job.lease_expires_at = expires_at
        job.last_heartbeat_at = now
        if details is not None:
            job.heartbeat_details = details
        return HeartbeatResult(cancel_requested=job.cancel_requested)

    async def release_step(
        self, job_id: str, worker_id: str, now: datetime, *, consume_attempt: bool
    ) -> bool:
        job = self._owned_job(job_id, worker_id)
        if job is None:
            return False
        if job.cancel_requested:
            self._delete_job(job)
            return True
        job.lease_owner = None
        job.lease_expires_at = None
        job.ready_at = min(job.ready_at, now)
        if not consume_attempt:
            job.attempt = max(job.attempt - 1, 0)
        self.notifier.publish(TOPIC_STEPS)
        return True

    async def next_step_ready_at(
        self,
        step_names: Collection[str] | None = None,
        queues: Collection[str] | None = None,
    ) -> datetime | None:
        candidates = [
            job.ready_at
            for job in self._jobs.values()
            if job.lease_owner is None
            and not job.cancel_requested
            and (step_names is None or job.step_name in step_names)
            and (queues is None or job.queue in queues)
        ]
        return min(candidates, default=None)

    async def get_step_jobs(self, run_id: str) -> list[StepJob]:
        run = self._run(run_id)
        return [copy.copy(self._jobs[job_id]) for _, job_id in sorted(run.step_jobs.items())]

    # ------------------------------------------------------------------ worker builds

    async def repin_run(self, run_id: str, build_id: str | None, now: datetime) -> ExecutionRecord:
        run = self._run(run_id)
        if run.record.is_closed:
            raise WorkflowClosedError(f"run {run_id} is closed; it cannot be repinned")
        run.record.pinned_build_id = build_id
        return copy.copy(run.record)

    async def declare_build_compatible(
        self, build_id: str, compatible_with: str, now: datetime
    ) -> None:
        # Idempotent: a declaration that already exists keeps its original created_at,
        # matching the SQL stores (whose INSERT is skipped once the row is there).
        self._build_compat.setdefault((build_id, compatible_with), now)

    async def revoke_build_compatibility(self, build_id: str, compatible_with: str) -> bool:
        return self._build_compat.pop((build_id, compatible_with), None) is not None

    async def list_build_compatibility(self) -> list[BuildCompatibility]:
        # Matches the SQL stores' `ORDER BY created_at DESC, build_id, compatible_with`
        # exactly, including the tiebreaker, so ties (a common case in tests that share
        # one clock) sort identically on every backend.
        ordered = sorted(
            self._build_compat.items(), key=lambda item: (-item[1].timestamp(), *item[0])
        )
        return [
            BuildCompatibility(build_id=b, compatible_with=c, created_at=at)
            for (b, c), at in ordered
        ]

    async def heartbeat_worker(
        self, worker_id: str, build_id: str, now: datetime, expires_at: datetime
    ) -> None:
        self._worker_presence[worker_id] = WorkerPresence(
            worker_id=worker_id, build_id=build_id, last_seen_at=now, expires_at=expires_at
        )

    async def list_worker_builds(self, now: datetime) -> list[WorkerPresence]:
        live = [p for p in self._worker_presence.values() if p.expires_at > now]
        return sorted(live, key=lambda p: (-p.last_seen_at.timestamp(), p.worker_id))

    async def count_pinned_runs(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for run in self._runs.values():
            record = run.record
            if not record.is_closed and record.pinned_build_id is not None:
                counts[record.pinned_build_id] = counts.get(record.pinned_build_id, 0) + 1
        return counts

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
            if run.record.is_closed:
                pass
            elif seq == DEADLINE_TIMER_SEQ:
                reason = "run exceeded its timeout"
                self._append(run, WorkflowTimedOut(reason=reason), now)
                self._close(run, CloseOutcome(ExecutionStatus.TIMED_OUT, reason=reason), now)
            else:
                self._append(run, TimerFired(seq=seq), now)
                self._mark_tick_pending(run, now)
            fired += 1
        return fired

    # ------------------------------------------------------------------ visibility

    async def query_executions(
        self, query: str, schema: SearchAttributes, now: datetime, limit: int = 100
    ) -> list[ExecutionRecord]:
        from loomwork.visibility import (
            compile_predicate,
            order_and_limit,
            parse_query,
        )

        parsed = parse_query(query)
        predicate = compile_predicate(parsed, schema, now)
        matches = [
            copy.copy(self._runs[run_id].record)
            for run_id in reversed(self._order)
            if predicate(
                self._runs[run_id].record,
                decode_attributes(self._attributes.get(run_id, {})),
            )
        ]
        return order_and_limit(matches, parsed, schema, now)[:limit]

    async def get_search_attributes(self, run_id: str) -> dict[str, object]:
        self._run(run_id)
        return decode_attributes(self._attributes.get(run_id, {}))

    # ------------------------------------------------------------------ outbox

    async def relay_outbox(self, now: datetime, limit: int = 100) -> int:
        # Only messages present at the start; ones enqueued while relaying (for example
        # by a termination) wait for the next call, as in the SQL stores.
        batch = min(limit, len(self._outbox))
        processed = 0
        while processed < batch:
            message = self._outbox.popleft()
            processed += 1
            if message.target_run_id is not None:
                target = self._runs.get(message.target_run_id)
            else:
                assert message.target_workflow_id is not None
                running = self._running_for(message.target_workflow_id)
                target = self._runs[running.run_id] if running else None
            if target is None or target.record.is_closed:
                continue
            if message.action is OutboxAction.APPEND_INPUT:
                assert message.event is not None
                self._append(target, message.event, now)
                self._mark_tick_pending(target, now)
            elif message.action is OutboxAction.REQUEST_CANCEL:
                await self.request_cancel(target.record.run_id, message.reason, now)
            else:
                await self.terminate(target.record.run_id, message.reason, now)
        return processed

    async def outbox_size(self) -> int:
        return len(self._outbox)

    async def list_children(self, run_id: str) -> list[ExecutionRecord]:
        self._run(run_id)
        return [
            copy.copy(self._runs[c].record)
            for c in self._children.get(run_id, ())
            if c in self._runs
        ]

    # ------------------------------------------------------------------ schedules

    async def create_schedule(self, state: ScheduleState, now: datetime) -> ScheduleState:
        if state.schedule_id in self._schedules:
            raise ScheduleExistsError(f"schedule {state.schedule_id!r} already exists")
        stored = state.with_(version=1, created_at=now, updated_at=now)
        self._schedules[state.schedule_id] = stored
        self.notifier.publish(TOPIC_SCHEDULES)
        return stored

    async def get_schedule(self, schedule_id: str) -> ScheduleState:
        try:
            return self._schedules[schedule_id]
        except KeyError:
            raise NotFoundError(f"schedule {schedule_id!r} does not exist") from None

    async def list_schedules(self, limit: int = 100) -> list[ScheduleState]:
        return [self._schedules[k] for k in sorted(self._schedules)[:limit]]

    def _check_version(self, schedule_id: str, expected: int) -> None:
        current = self._schedules.get(schedule_id)
        if current is None:
            raise NotFoundError(f"schedule {schedule_id!r} does not exist")
        if current.version != expected:
            raise ScheduleConflictError(
                f"schedule {schedule_id!r} is at version {current.version}, expected {expected}"
            )

    async def update_schedule(
        self, state: ScheduleState, expected_version: int, now: datetime
    ) -> ScheduleState:
        self._check_version(state.schedule_id, expected_version)
        stored = state.with_(version=expected_version + 1, updated_at=now)
        self._schedules[state.schedule_id] = stored
        self.notifier.publish(TOPIC_SCHEDULES)
        return stored

    async def delete_schedule(self, schedule_id: str) -> bool:
        return self._schedules.pop(schedule_id, None) is not None

    async def due_schedules(self, now: datetime, limit: int = 100) -> list[ScheduleState]:
        due = [
            s
            for s in self._schedules.values()
            if s.buffered is not None
            or (not s.paused and s.next_fire_at is not None and s.next_fire_at <= now)
        ]
        return sorted(due, key=lambda s: s.schedule_id)[:limit]

    async def fire_schedule(
        self,
        state: ScheduleState,
        expected_version: int,
        starts: Sequence[ChildSpec],
        outbox: Sequence[OutboxMessage],
        now: datetime,
    ) -> ScheduleState:
        self._check_version(state.schedule_id, expected_version)
        conflicts: set[str] = set()
        reuse_rejected: set[str] = set()
        attached: dict[str, str] = {}
        terminated: set[str] = set()
        for spec in starts:
            child = spec.record
            running = self._running_for(child.workflow_id)
            if running is not None:
                if spec.id_conflict_policy is IdConflictPolicy.USE_EXISTING:
                    attached[child.run_id] = running.run_id
                    continue
                if spec.id_conflict_policy is IdConflictPolicy.TERMINATE_EXISTING:
                    running_run = self._runs[running.run_id]
                    reason = "superseded by schedule id conflict policy"
                    self._append(running_run, WorkflowTerminated(reason=reason), now)
                    self._close(
                        running_run, CloseOutcome(ExecutionStatus.TERMINATED, reason=reason), now
                    )
                    terminated.add(running.run_id)
                else:
                    conflicts.add(child.run_id)
                    continue
            elif spec.id_reuse_policy is not IdReusePolicy.ALLOW_DUPLICATE:
                run_ids = self._by_workflow_id.get(child.workflow_id)
                if run_ids:
                    latest = self._runs[run_ids[-1]].record
                    if not id_reuse_allowed(spec.id_reuse_policy, latest.status):
                        conflicts.add(child.run_id)
                        reuse_rejected.add(child.run_id)
                        continue
            if child.run_id in self._runs:
                conflicts.add(child.run_id)
                continue
            self._insert_run(child, spec.started, now)
        self._enqueue(outbox)
        stored = drop_unstarted(state, conflicts, reuse_rejected=reuse_rejected)
        stored = attach_unstarted(stored, attached)
        stored = reconcile_running(stored, terminated)
        stored = stored.with_(version=expected_version + 1, updated_at=now)
        self._schedules[state.schedule_id] = stored
        self.notifier.publish(TOPIC_SCHEDULES)
        return stored

    async def next_schedule_at(self) -> datetime | None:
        times = [
            s.next_fire_at
            for s in self._schedules.values()
            if not s.paused and s.next_fire_at is not None
        ]
        return min(times, default=None)

    # ------------------------------------------------------------------ retention

    async def delete_closed_before(self, cutoff: datetime, limit: int = 1000) -> int:
        victims = sorted(
            (
                run
                for run in self._runs.values()
                if run.record.is_closed
                and run.record.closed_at is not None
                and run.record.closed_at < cutoff
            ),
            key=lambda run: run.record.closed_at or cutoff,
        )[:limit]
        for run in victims:
            run_id = run.record.run_id
            del self._runs[run_id]
            self._attributes.pop(run_id, None)
            self._order.remove(run_id)
            siblings = self._by_workflow_id[run.record.workflow_id]
            siblings.remove(run_id)
            if not siblings:
                del self._by_workflow_id[run.record.workflow_id]
        return len(victims)

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


def _claim_order(job: StepJob) -> tuple[int, datetime, str]:
    return (-job.priority, job.ready_at, job.job_id)


def _maybe_pin(record: ExecutionRecord, build_id: str | None) -> None:
    """Set ``record.pinned_build_id`` in the same commit as its first tick, mirroring
    ``SQLStore.complete_tick`` (see ``docs/adr/0006-worker-build-versioning.md``)."""
    if (
        record.versioning_policy is VersioningPolicy.PINNED
        and record.pinned_build_id is None
        and build_id
    ):
        record.pinned_build_id = build_id


def _event_size(event: Event) -> int:
    """Approximate encoded size of ``event``, for ``EngineConfig.history_limit_bytes``.

    Matches the shape (not the exact bytes) of what the SQL stores persist per event, so
    the limit means roughly the same thing on every backend without requiring this
    non-durable store to actually serialize anything.
    """
    return len(json.dumps(event_to_dict(event), default=str))
