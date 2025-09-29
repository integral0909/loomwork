"""A :class:`Store` implemented once in portable SQL.

Backends supply a :class:`SqlTransaction` (connection handling, placeholder style,
deferred notifications), timestamp conversion and the row-locking clauses of their
dialect. Everything else, including every state transition, lives here so all SQL
backends behave identically.

Lock ordering
    Every method that locks more than one row locks the **execution row first**, then
    step-job or timer rows. Keeping one global order rules out deadlocks between, say,
    a tick commit cancelling a timer and the timer service firing it.
"""

from __future__ import annotations

import abc
import asyncio
import dataclasses
import random
from collections.abc import Collection, Iterable, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from loomwork.codec.payload import Payload
from loomwork.errors import (
    LeaseLostError,
    NotFoundError,
    ScheduleConflictError,
    ScheduleExistsError,
    WorkflowAlreadyStartedError,
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
from loomwork.history.serde import event_from_dict
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint
from loomwork.schedules.model import ScheduleState
from loomwork.visibility.attributes import attribute_rows, decode_attributes

if TYPE_CHECKING:
    from loomwork.visibility.schema import SearchAttributes
from loomwork.store.base import Store, id_reuse_allowed
from loomwork.store.closing import (
    DEADLINE_TIMER_SEQ,
    child_start_failed_event,
    closing_messages,
    drop_unstarted,
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
    ChildSpec,
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    HeartbeatResult,
    OutboxAction,
    OutboxMessage,
    StepJob,
    TickOutcome,
)
from loomwork.store.reset import ResetPlan, plan_reset, reconstruct_step_job, reconstruct_timer
from loomwork.store.sql import codec
from loomwork.store.sql.migrations import Dialect

Row = codec.Row


class SqlTransaction(Protocol):
    """One database transaction. SQL uses ``?`` placeholders; backends translate."""

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        """Run a statement and return the affected row count."""
        ...

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> Row | None: ...

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[Row]: ...

    def notify(self, topic: str) -> None:
        """Publish ``topic`` once the transaction commits."""
        ...


#: create_execution's retry budget for IdConflictPolicy.TERMINATE_EXISTING racing a
#: concurrent creation of the same, previously nonexistent, workflow id. A jittered
#: backoff between attempts (below) keeps many simultaneous racers from repeatedly
#: colliding in lockstep.
_MAX_CREATE_ATTEMPTS = 20


def _in_clause(column: str, values: Collection[str]) -> tuple[str, list[str]]:
    items = sorted(values)
    return f"{column} IN ({', '.join('?' for _ in items)})", items


class SQLStore(Store, abc.ABC):
    """Shared implementation of the store contract for SQL databases."""

    dialect: ClassVar[Dialect]
    #: Appended to single-row SELECTs that must lock the row until commit.
    FOR_UPDATE: ClassVar[str] = ""
    #: Appended to claim queries so concurrent workers skip rows being claimed.
    SKIP_LOCKED: ClassVar[str] = ""

    # ------------------------------------------------------------------ backend hooks

    @abc.abstractmethod
    def transaction(self) -> AbstractAsyncContextManager[SqlTransaction]:
        """An async context manager yielding a :class:`SqlTransaction`."""

    @abc.abstractmethod
    def ts_in(self, value: datetime | None) -> Any:
        """Convert a datetime to the backend's column representation."""

    @abc.abstractmethod
    def ts_out(self, value: Any) -> datetime | None:
        """Convert a column value back to an aware UTC datetime."""

    @abc.abstractmethod
    def is_unique_violation(self, exc: BaseException) -> bool:
        """Whether ``exc`` is a unique-constraint violation."""

    # ------------------------------------------------------------------ row helpers

    def _record(self, row: Row) -> ExecutionRecord:
        return codec.execution_from_row(row, self.ts_out)

    def _job(self, row: Row) -> StepJob:
        return codec.step_job_from_row(row, self.ts_out)

    async def _load(self, tx: SqlTransaction, run_id: str, *, lock: bool) -> ExecutionRecord:
        suffix = self.FOR_UPDATE if lock else ""
        row = await tx.fetchone(f"SELECT * FROM lw_executions WHERE run_id = ?{suffix}", (run_id,))
        if row is None:
            raise NotFoundError(f"workflow run {run_id!r} does not exist")
        return self._record(row)

    async def _write(self, tx: SqlTransaction, rec: ExecutionRecord) -> None:
        await tx.execute(
            """
            UPDATE lw_executions SET
                status = ?, closed_at = ?, last_event_id = ?, last_input_id = ?,
                consumed_through = ?, tick_pending = ?, tick_ready_at = ?, tick_failures = ?,
                last_tick_error = ?, lease_owner = ?, lease_expires_at = ?,
                cancel_requested = ?, result = ?, failure = ?, close_reason = ?,
                continued_run_id = ?, history_bytes = ?
            WHERE run_id = ?
            """,
            (
                rec.status.value,
                self.ts_in(rec.closed_at),
                rec.last_event_id,
                rec.last_input_id,
                rec.consumed_through,
                rec.tick_pending,
                self.ts_in(rec.tick_ready_at),
                rec.tick_failures,
                rec.last_tick_error,
                rec.lease_owner,
                self.ts_in(rec.lease_expires_at),
                rec.cancel_requested,
                codec.payload_to_json(rec.result),
                codec.failure_to_json(rec.failure),
                rec.close_reason,
                rec.continued_run_id,
                rec.history_bytes,
                rec.run_id,
            ),
        )

    async def _append(
        self, tx: SqlTransaction, rec: ExecutionRecord, event: Event, now: datetime
    ) -> Event:
        stored = dataclasses.replace(event, event_id=rec.last_event_id + 1, timestamp=now)
        body = codec.event_body_to_json(stored)
        await tx.execute(
            "INSERT INTO lw_events (run_id, event_id, kind, created_at, body) "
            "VALUES (?, ?, ?, ?, ?)",
            (rec.run_id, stored.event_id, stored.kind, self.ts_in(now), body),
        )
        rec.last_event_id = stored.event_id
        rec.history_bytes += len(body)
        if stored.is_input:
            rec.last_input_id = stored.event_id
        tx.notify(run_topic(rec.run_id))
        return stored

    async def _append_many(
        self, tx: SqlTransaction, rec: ExecutionRecord, events: Sequence[Event], now: datetime
    ) -> list[Event]:
        """Append several events with one multi-row INSERT."""
        stored: list[Event] = []
        params: list[Any] = []
        for event in events:
            item = dataclasses.replace(event, event_id=rec.last_event_id + 1, timestamp=now)
            rec.last_event_id = item.event_id
            if item.is_input:
                rec.last_input_id = item.event_id
            stored.append(item)
            body = codec.event_body_to_json(item)
            rec.history_bytes += len(body)
            params.extend((rec.run_id, item.event_id, item.kind, self.ts_in(now), body))
        if stored:
            rows = ", ".join("(?, ?, ?, ?, ?)" for _ in stored)
            await tx.execute(
                f"INSERT INTO lw_events (run_id, event_id, kind, created_at, body) VALUES {rows}",
                params,
            )
            tx.notify(run_topic(rec.run_id))
        return stored

    @staticmethod
    def _refresh_tick_pending(tx: SqlTransaction, rec: ExecutionRecord, now: datetime) -> None:
        if rec.is_closed:
            return
        if rec.last_input_id > rec.consumed_through:
            if not rec.tick_pending:
                rec.tick_pending = True
                rec.tick_ready_at = now
            tx.notify(TOPIC_TICKS)
        else:
            rec.tick_pending = False
            rec.tick_ready_at = None

    async def _close(
        self, tx: SqlTransaction, rec: ExecutionRecord, outcome: CloseOutcome, now: datetime
    ) -> None:
        rec.status = outcome.status
        rec.closed_at = now
        rec.result = outcome.result
        rec.failure = outcome.failure
        rec.close_reason = outcome.reason
        rec.continued_run_id = outcome.continued_run_id
        rec.tick_pending = False
        rec.tick_ready_at = None
        rec.lease_owner = None
        rec.lease_expires_at = None
        await tx.execute("DELETE FROM lw_step_jobs WHERE run_id = ?", (rec.run_id,))
        await tx.execute("DELETE FROM lw_timers WHERE run_id = ?", (rec.run_id,))
        rows = await tx.fetchall(
            "SELECT * FROM lw_executions WHERE parent_run_id = ? AND status = ? ORDER BY seq",
            (rec.run_id, ExecutionStatus.RUNNING.value),
        )
        await self._enqueue(tx, closing_messages(rec, [self._record(r) for r in rows]), now)
        tx.notify(closed_topic(rec.run_id))

    async def _enqueue(
        self, tx: SqlTransaction, messages: Iterable[OutboxMessage], now: datetime
    ) -> None:
        wrote = False
        for message in messages:
            event = message.event
            await tx.execute(
                "INSERT INTO lw_outbox (action, target_run_id, target_workflow_id, event_kind, "
                "event_body, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    message.action.value,
                    message.target_run_id,
                    message.target_workflow_id,
                    event.kind if event is not None else None,
                    codec.event_body_to_json(event) if event is not None else None,
                    message.reason,
                    self.ts_in(now),
                ),
            )
            wrote = True
        if wrote:
            tx.notify(TOPIC_OUTBOX)

    @staticmethod
    def _lease_free(owner: str | None, expires: datetime | None, now: datetime) -> bool:
        return owner is None or (expires is not None and expires <= now)

    # ------------------------------------------------------------------ executions

    async def _insert_run(
        self,
        tx: SqlTransaction,
        rec: ExecutionRecord,
        started: Event,
        now: datetime,
        extra_inputs: Sequence[Event] = (),
        ready_at: datetime | None = None,
    ) -> Event:
        await tx.execute(
            """
            INSERT INTO lw_executions (
                run_id, workflow_id, workflow_name, status, created_at,
                parent_run_id, parent_seq, parent_close_policy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rec.run_id,
                rec.workflow_id,
                rec.workflow_name,
                rec.status.value,
                self.ts_in(rec.created_at),
                rec.parent_run_id,
                rec.parent_seq,
                rec.parent_close_policy,
            ),
        )
        initial = getattr(started, "search_attributes", None)
        if initial:
            await self._write_attributes(tx, rec.run_id, initial)
        event = await self._append(tx, rec, started, now)
        for extra in extra_inputs:
            if not extra.is_input:
                raise ValueError(f"{type(extra).__name__} is not an input event")
            await self._append(tx, rec, extra, now)
        self._refresh_tick_pending(tx, rec, now)
        if ready_at is not None and ready_at > now:
            rec.tick_ready_at = ready_at  # a retried run waits out its backoff
        await self._write(tx, rec)
        deadline = run_deadline(started, now)
        if deadline is not None:
            await tx.execute(
                "INSERT INTO lw_timers (run_id, seq, fire_at) VALUES (?, ?, ?)",
                (rec.run_id, DEADLINE_TIMER_SEQ, self.ts_in(deadline)),
            )
            tx.notify(TOPIC_TIMERS)
        return event

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
        rec = dataclasses.replace(record)
        # `SELECT ... FOR UPDATE` locks a running row if one exists, but locks nothing
        # when none does: two callers can both see "no running execution" and both reach
        # the insert below before either commits. TERMINATE_EXISTING must still win that
        # race deterministically (its whole point is "my run replaces whoever is running,
        # no matter what"), so on a unique violation it loops back: the next iteration's
        # SELECT is guaranteed to see the row the other caller just committed.
        for _ in range(_MAX_CREATE_ATTEMPTS):
            try:
                async with self.transaction() as tx:
                    running_row = await tx.fetchone(
                        f"SELECT * FROM lw_executions WHERE workflow_id = ? AND status = ?"
                        f"{self.FOR_UPDATE}",
                        (rec.workflow_id, ExecutionStatus.RUNNING.value),
                    )
                    if running_row is not None:
                        running = self._record(running_row)
                        if id_conflict_policy is IdConflictPolicy.USE_EXISTING:
                            return running, started
                        if id_conflict_policy is IdConflictPolicy.TERMINATE_EXISTING:
                            reason = "superseded by id conflict policy"
                            await self._append(tx, running, WorkflowTerminated(reason=reason), now)
                            await self._close(
                                tx,
                                running,
                                CloseOutcome(ExecutionStatus.TERMINATED, reason=reason),
                                now,
                            )
                            await self._write(tx, running)
                        else:
                            raise WorkflowAlreadyStartedError(rec.workflow_id, running.run_id)
                    elif id_reuse_policy is not IdReusePolicy.ALLOW_DUPLICATE:
                        latest_row = await tx.fetchone(
                            "SELECT * FROM lw_executions WHERE workflow_id = ? "
                            "ORDER BY seq DESC LIMIT 1",
                            (rec.workflow_id,),
                        )
                        if latest_row is not None:
                            latest = self._record(latest_row)
                            if not id_reuse_allowed(id_reuse_policy, latest.status):
                                raise WorkflowIdReuseRejectedError(
                                    rec.workflow_id, latest.run_id, id_reuse_policy.value
                                )
                    if await tx.fetchone(
                        "SELECT 1 FROM lw_executions WHERE run_id = ?", (rec.run_id,)
                    ):
                        raise WorkflowAlreadyStartedError(rec.workflow_id, rec.run_id)
                    event = await self._insert_run(tx, rec, started, now, extra_inputs)
                return rec, event
            except (WorkflowAlreadyStartedError, WorkflowIdReuseRejectedError):
                raise
            except Exception as exc:
                if not self.is_unique_violation(exc):
                    raise
                # Lost a race with a concurrent start of the same workflow id.
                if id_conflict_policy is IdConflictPolicy.TERMINATE_EXISTING:
                    await asyncio.sleep(random.uniform(0, 0.002))
                    continue
                existing = await self.find_execution(rec.workflow_id)
                if id_conflict_policy is IdConflictPolicy.USE_EXISTING:
                    return existing, started
                raise WorkflowAlreadyStartedError(rec.workflow_id, existing.run_id) from exc
        raise WorkflowAlreadyStartedError(rec.workflow_id, "<contended>")

    async def get_execution(self, run_id: str) -> ExecutionRecord:
        async with self.transaction() as tx:
            return await self._load(tx, run_id, lock=False)

    async def find_execution(self, workflow_id: str) -> ExecutionRecord:
        async with self.transaction() as tx:
            row = await tx.fetchone(
                "SELECT * FROM lw_executions WHERE workflow_id = ? ORDER BY seq DESC LIMIT 1",
                (workflow_id,),
            )
        if row is None:
            raise NotFoundError(f"no run exists for workflow id {workflow_id!r}")
        return self._record(row)

    async def list_executions(
        self,
        *,
        workflow_name: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if workflow_name is not None:
            clauses.append("workflow_name = ?")
            params.append(workflow_name)
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        async with self.transaction() as tx:
            rows = await tx.fetchall(
                f"SELECT * FROM lw_executions {where} ORDER BY seq DESC LIMIT ?", params
            )
        return [self._record(row) for row in rows]

    # ------------------------------------------------------------------ history

    async def load_history(
        self, run_id: str, after_id: int = 0, limit: int | None = None
    ) -> list[Event]:
        sql = (
            "SELECT event_id, kind, created_at, body FROM lw_events "
            "WHERE run_id = ? AND event_id > ? ORDER BY event_id"
        )
        params: list[Any] = [run_id, after_id]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        async with self.transaction() as tx:
            rows = await tx.fetchall(sql, params)
            if not rows and not await tx.fetchone(
                "SELECT 1 FROM lw_executions WHERE run_id = ?", (run_id,)
            ):
                raise NotFoundError(f"workflow run {run_id!r} does not exist")
        return [
            codec.event_from_row(
                int(row["event_id"]), row["kind"], self._required_ts(row["created_at"]), row["body"]
            )
            for row in rows
        ]

    def _required_ts(self, value: Any) -> datetime:
        converted = self.ts_out(value)
        if converted is None:
            raise ValueError("unexpected NULL timestamp")
        return converted

    async def append_input(self, run_id: str, event: Event, now: datetime) -> Event | None:
        if not event.is_input:
            raise ValueError(f"{type(event).__name__} is not an input event")
        async with self.transaction() as tx:
            rec = await self._load(tx, run_id, lock=True)
            if rec.is_closed:
                return None
            stored = await self._append(tx, rec, event, now)
            self._refresh_tick_pending(tx, rec, now)
            await self._write(tx, rec)
            return stored

    async def request_cancel(self, run_id: str, reason: str, now: datetime) -> bool:
        async with self.transaction() as tx:
            rec = await self._load(tx, run_id, lock=True)
            if rec.is_closed:
                return False
            if not rec.cancel_requested:
                rec.cancel_requested = True
                await self._append(tx, rec, CancelRequested(reason=reason), now)
                self._refresh_tick_pending(tx, rec, now)
                await self._write(tx, rec)
            return True

    async def terminate(self, run_id: str, reason: str, now: datetime) -> bool:
        async with self.transaction() as tx:
            rec = await self._load(tx, run_id, lock=True)
            if rec.is_closed:
                return False
            await self._append(tx, rec, WorkflowTerminated(reason=reason), now)
            await self._close(tx, rec, CloseOutcome(ExecutionStatus.TERMINATED, reason=reason), now)
            await self._write(tx, rec)
            return True

    async def reset_run(
        self,
        run_id: str,
        new_run_id: str,
        to: ResetPoint,
        reason: str,
        now: datetime,
    ) -> tuple[ExecutionRecord, ExecutionRecord]:
        # Validate `to` against the run's current history before mutating anything:
        # ticks only accumulate, so if this succeeds the second computation below
        # (against a history that may have grown further by then) is guaranteed to
        # succeed too.
        plan_reset(await self.load_history(run_id), to)

        # Two transactions, never locking both execution rows at once (ADR-0004): once
        # the old run is closed it is immutable, so the history read after this one
        # can no longer change under it.
        async with self.transaction() as tx:
            rec = await self._load(tx, run_id, lock=True)
            was_running = not rec.is_closed
            if was_running:
                await self._append(tx, rec, WorkflowTerminated(reason=reason), now)
                await self._close(
                    tx,
                    rec,
                    CloseOutcome(
                        ExecutionStatus.TERMINATED, reason=reason, continued_run_id=new_run_id
                    ),
                    now,
                )
            else:
                # `rec` itself cannot be racing a tick (closed runs reject everything),
                # but some other run of the same workflow id may have started since it
                # closed; the insert below still has the partial unique index as a
                # last-resort safety net against a concurrent one.
                other_running = await tx.fetchone(
                    "SELECT run_id FROM lw_executions WHERE workflow_id = ? AND status = ?",
                    (rec.workflow_id, ExecutionStatus.RUNNING.value),
                )
                if other_running is not None:
                    raise WorkflowAlreadyStartedError(rec.workflow_id, other_running["run_id"])
                rec.continued_run_id = new_run_id
            await self._write(tx, rec)

        # Exclude the WorkflowTerminated just appended, if any: it is not part of the
        # history being reset, only a record of this call having closed the run.
        history = await self.load_history(run_id)
        if was_running:
            history = history[:-1]
        plan = plan_reset(history, to)
        kept = [e for e in history if e.event_id <= plan.cutoff_event_id]
        started = kept[0]
        assert isinstance(started, WorkflowStarted)

        new_rec = ExecutionRecord(
            run_id=new_run_id,
            workflow_id=rec.workflow_id,
            workflow_name=rec.workflow_name,
            status=ExecutionStatus.RUNNING,
            created_at=now,
            parent_run_id=rec.parent_run_id,
            parent_seq=rec.parent_seq,
            parent_close_policy=rec.parent_close_policy,
            reset_from_run_id=run_id,
        )
        try:
            async with self.transaction() as tx:
                await self._insert_reset_successor(tx, new_rec, kept, plan, started, now)
        except Exception as exc:
            if self.is_unique_violation(exc):
                # Lost a race with a concurrent start of the same workflow id.
                existing = await self.find_execution(new_rec.workflow_id)
                raise WorkflowAlreadyStartedError(new_rec.workflow_id, existing.run_id) from exc
            raise
        return rec, new_rec

    async def _insert_reset_successor(
        self,
        tx: SqlTransaction,
        new_rec: ExecutionRecord,
        kept: Sequence[Event],
        plan: ResetPlan,
        started: WorkflowStarted,
        now: datetime,
    ) -> None:
        """The second transaction of :meth:`reset_run`: create ``new_rec`` with a
        history that is ``kept`` copied verbatim, its reconstructed step jobs and
        timers, and ``plan.redeliver`` appended as fresh inputs."""
        await tx.execute(
            """
            INSERT INTO lw_executions (
                run_id, workflow_id, workflow_name, status, created_at,
                parent_run_id, parent_seq, parent_close_policy, reset_from_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_rec.run_id,
                new_rec.workflow_id,
                new_rec.workflow_name,
                new_rec.status.value,
                self.ts_in(new_rec.created_at),
                new_rec.parent_run_id,
                new_rec.parent_seq,
                new_rec.parent_close_policy,
                new_rec.reset_from_run_id,
            ),
        )
        if kept:
            params: list[Any] = []
            for event in kept:
                body = codec.event_body_to_json(event)
                new_rec.history_bytes += len(body)
                params.extend(
                    (new_rec.run_id, event.event_id, event.kind, self.ts_in(event.timestamp), body)
                )
            rows = ", ".join("(?, ?, ?, ?, ?)" for _ in kept)
            await tx.execute(
                f"INSERT INTO lw_events (run_id, event_id, kind, created_at, body) VALUES {rows}",
                params,
            )
        new_rec.last_event_id = plan.cutoff_event_id
        new_rec.last_input_id = max((e.event_id for e in kept if e.is_input), default=0)
        new_rec.consumed_through = plan.consumed_through

        if plan.search_attributes:
            await self._write_attributes(tx, new_rec.run_id, plan.search_attributes)
        for scheduled in plan.open_steps:
            await self._insert_job(
                tx,
                reconstruct_step_job(
                    run_id=new_rec.run_id,
                    workflow_id=new_rec.workflow_id,
                    workflow_name=new_rec.workflow_name,
                    scheduled=scheduled,
                    now=now,
                ),
            )
        for timer_started in plan.open_timers:
            timer = reconstruct_timer(new_rec.run_id, timer_started)
            await tx.execute(
                "INSERT INTO lw_timers (run_id, seq, fire_at) VALUES (?, ?, ?)",
                (timer.run_id, timer.seq, self.ts_in(timer.fire_at)),
            )
        deadline = run_deadline(started, now)
        if deadline is not None:
            await tx.execute(
                "INSERT INTO lw_timers (run_id, seq, fire_at) VALUES (?, ?, ?)",
                (new_rec.run_id, DEADLINE_TIMER_SEQ, self.ts_in(deadline)),
            )

        self._refresh_tick_pending(tx, new_rec, now)
        await self._write(tx, new_rec)
        for redelivered in plan.redeliver:
            await self._append(tx, new_rec, redelivered, now)
        if plan.redeliver:
            self._refresh_tick_pending(tx, new_rec, now)
            await self._write(tx, new_rec)

        if plan.open_steps:
            tx.notify(TOPIC_STEPS)
        if plan.open_timers or deadline is not None:
            tx.notify(TOPIC_TIMERS)

    # ------------------------------------------------------------------ ticks

    async def claim_tick(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        workflow_names: Collection[str] | None = None,
    ) -> ExecutionRecord | None:
        if workflow_names is not None and not workflow_names:
            return None
        sql = (
            "SELECT * FROM lw_executions WHERE tick_pending = ? AND status = ? "
            "AND tick_ready_at <= ? AND (lease_owner IS NULL OR lease_expires_at <= ?)"
        )
        now_db = self.ts_in(now)
        params: list[Any] = [True, ExecutionStatus.RUNNING.value, now_db, now_db]
        if workflow_names is not None:
            clause, names = _in_clause("workflow_name", workflow_names)
            sql += f" AND {clause}"
            params.extend(names)
        sql += f" ORDER BY tick_ready_at, run_id LIMIT 1{self.SKIP_LOCKED}"
        # One statement: pick the candidate (skipping rows other workers are claiming)
        # and lease it.
        claim = (
            "UPDATE lw_executions SET lease_owner = ?, lease_expires_at = ? "
            f"WHERE run_id = ({sql.replace('SELECT *', 'SELECT run_id', 1)}) RETURNING *"
        )
        async with self.transaction() as tx:
            row = await tx.fetchone(claim, [worker_id, self.ts_in(now + lease), *params])
        return None if row is None else self._record(row)

    async def complete_tick(
        self, run_id: str, worker_id: str, outcome: TickOutcome, now: datetime
    ) -> list[Event]:
        async with self.transaction() as tx:
            rec = await self._load(tx, run_id, lock=True)
            if rec.lease_owner != worker_id:
                raise LeaseLostError(f"worker {worker_id} does not hold the lease on run {run_id}")
            if rec.is_closed:
                raise LeaseLostError(f"run {run_id} was closed while the tick was running")

            appended: list[Event] = []
            if outcome.consumed_through > rec.consumed_through or outcome.decisions:
                tick = TickCompleted(
                    consumed_through=max(outcome.consumed_through, rec.consumed_through),
                    time=outcome.time,
                    worker_id=worker_id,
                    code_version=outcome.code_version,
                )
                appended.extend(await self._append_many(tx, rec, [tick, *outcome.decisions], now))
            rec.consumed_through = max(rec.consumed_through, outcome.consumed_through)
            rec.lease_owner = None
            rec.lease_expires_at = None
            rec.tick_failures = 0
            rec.last_tick_error = None

            await self._enqueue(tx, outcome.outbox, now)
            for child in outcome.new_children:
                await self._start_child(tx, rec, child.record, child.started, now)

            if outcome.search_attributes:
                await self._write_attributes(tx, run_id, outcome.search_attributes)
            if outcome.close is not None:
                await self._close(tx, rec, outcome.close, now)
                # Write the closed status first: the successor reuses the workflow id,
                # which the partial unique index only allows once this run is closed.
                await self._write(tx, rec)
                if outcome.continuation is not None:
                    successor = outcome.continuation
                    await self._insert_run(
                        tx,
                        dataclasses.replace(successor.record),
                        successor.started,
                        now,
                        ready_at=outcome.continuation_ready_at,
                    )
                    await self._inherit_attributes(
                        tx, run_id, successor.record.run_id, successor.started
                    )
                return appended

            for seq in outcome.cancel_steps:
                await self._cancel_step(tx, run_id, seq, now)
            for job in outcome.new_steps:
                await self._insert_job(tx, job)
            for seq in outcome.cancel_timers:
                await tx.execute(
                    "DELETE FROM lw_timers WHERE run_id = ? AND seq = ?", (run_id, seq)
                )
            for timer in outcome.new_timers:
                await tx.execute(
                    "INSERT INTO lw_timers (run_id, seq, fire_at) VALUES (?, ?, ?)",
                    (timer.run_id, timer.seq, self.ts_in(timer.fire_at)),
                )
            if outcome.new_steps:
                tx.notify(TOPIC_STEPS)
            if outcome.new_timers or outcome.cancel_timers:
                tx.notify(TOPIC_TIMERS)
            rec.tick_pending = False
            self._refresh_tick_pending(tx, rec, now)
            await self._write(tx, rec)
            return appended

    async def _start_child(
        self,
        tx: SqlTransaction,
        parent: ExecutionRecord,
        child: ExecutionRecord,
        started: Event,
        now: datetime,
    ) -> None:
        running = await tx.fetchone(
            "SELECT run_id FROM lw_executions WHERE workflow_id = ? AND status = ?",
            (child.workflow_id, ExecutionStatus.RUNNING.value),
        )
        clash = running is not None or await tx.fetchone(
            "SELECT 1 FROM lw_executions WHERE run_id = ?", (child.run_id,)
        )
        if clash:
            assert child.parent_seq is not None
            await self._append(
                tx,
                parent,
                child_start_failed_event(
                    child.parent_seq,
                    child.workflow_name,
                    child.workflow_id,
                    running["run_id"] if running is not None else child.run_id,
                ),
                now,
            )
            return
        await self._insert_run(tx, dataclasses.replace(child), started, now)

    async def _cancel_step(self, tx: SqlTransaction, run_id: str, seq: int, now: datetime) -> None:
        row = await tx.fetchone(
            f"SELECT * FROM lw_step_jobs WHERE run_id = ? AND seq = ?{self.FOR_UPDATE}",
            (run_id, seq),
        )
        if row is None:
            return
        job = self._job(row)
        if self._lease_free(job.lease_owner, job.lease_expires_at, now):
            await tx.execute("DELETE FROM lw_step_jobs WHERE job_id = ?", (job.job_id,))
        else:
            await tx.execute(
                "UPDATE lw_step_jobs SET cancel_requested = ? WHERE job_id = ?", (True, job.job_id)
            )

    async def _insert_job(self, tx: SqlTransaction, job: StepJob) -> None:
        await tx.execute(
            """
            INSERT INTO lw_step_jobs (
                job_id, run_id, workflow_id, workflow_name, seq, step_name, args, options,
                scheduled_at, ready_at, deadline, attempt, lease_owner, lease_expires_at,
                last_failure, cancel_requested, queue, priority, heartbeat_details,
                last_heartbeat_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job.job_id,
                job.run_id,
                job.workflow_id,
                job.workflow_name,
                job.seq,
                job.step_name,
                codec.payloads_to_json(job.args),
                codec.options_to_json(job.options),
                self.ts_in(job.scheduled_at),
                self.ts_in(job.ready_at),
                self.ts_in(job.deadline),
                job.attempt,
                job.lease_owner,
                self.ts_in(job.lease_expires_at),
                codec.failure_to_json(job.last_failure),
                job.cancel_requested,
                job.queue,
                job.priority,
                codec.payload_to_json(job.heartbeat_details),
                self.ts_in(job.last_heartbeat_at),
            ),
        )

    async def fail_tick(
        self,
        run_id: str,
        worker_id: str,
        error: str,
        retry_at: datetime,
        now: datetime,
    ) -> None:
        async with self.transaction() as tx:
            rec = await self._load(tx, run_id, lock=True)
            if rec.lease_owner != worker_id:
                raise LeaseLostError(f"worker {worker_id} does not hold the lease on run {run_id}")
            rec.lease_owner = None
            rec.lease_expires_at = None
            rec.tick_failures += 1
            rec.last_tick_error = error
            if not rec.is_closed:
                rec.tick_pending = True
                rec.tick_ready_at = retry_at
                tx.notify(TOPIC_TICKS)
            await self._write(tx, rec)

    # ------------------------------------------------------------------ steps

    async def claim_step(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        step_names: Collection[str] | None = None,
        queues: Collection[str] | None = None,
    ) -> StepJob | None:
        if (step_names is not None and not step_names) or (queues is not None and not queues):
            return None
        now_db = self.ts_in(now)
        async with self.transaction() as tx:
            sql = (
                "SELECT * FROM lw_step_jobs WHERE ready_at <= ? AND cancel_requested = ? "
                "AND (lease_owner IS NULL OR lease_expires_at <= ?)"
            )
            params: list[Any] = [now_db, False, now_db]
            sql, params = self._job_filters(sql, params, step_names, queues)
            sql += f" ORDER BY priority DESC, ready_at, job_id LIMIT 1{self.SKIP_LOCKED}"
            candidate = sql.replace("SELECT *", "SELECT job_id", 1)
            claim = (
                "UPDATE lw_step_jobs SET attempt = attempt + 1, lease_owner = ?, "
                f"lease_expires_at = ? WHERE job_id = ({candidate}) RETURNING *"
            )
            row = await tx.fetchone(claim, [worker_id, self.ts_in(now + lease), *params])
            if row is None:
                # Idle: a good moment to drop jobs whose workflow cancelled them while an
                # attempt was running and whose worker never reported back.
                await tx.execute(
                    "DELETE FROM lw_step_jobs WHERE cancel_requested = ? "
                    "AND (lease_owner IS NULL OR lease_expires_at <= ?)",
                    (True, now_db),
                )
                return None
            return self._job(row)

    async def _owned_job(self, tx: SqlTransaction, job_id: str, worker_id: str) -> StepJob | None:
        row = await tx.fetchone(
            f"SELECT * FROM lw_step_jobs WHERE job_id = ?{self.FOR_UPDATE}", (job_id,)
        )
        if row is None:
            return None
        job = self._job(row)
        return job if job.lease_owner == worker_id else None

    async def finish_step(
        self,
        job_id: str,
        worker_id: str,
        event: StepCompleted | StepFailed,
        now: datetime,
    ) -> bool:
        async with self.transaction() as tx:
            peek = await tx.fetchone("SELECT run_id FROM lw_step_jobs WHERE job_id = ?", (job_id,))
            if peek is None:
                return False
            # Lock order: execution row, then the job row (locked by the DELETE).
            rec = await self._load(tx, peek["run_id"], lock=True)
            deleted = await tx.execute(
                "DELETE FROM lw_step_jobs WHERE job_id = ? AND lease_owner = ?",
                (job_id, worker_id),
            )
            if not deleted:
                return False
            if not rec.is_closed:
                await self._append(tx, rec, event, now)
                self._refresh_tick_pending(tx, rec, now)
                await self._write(tx, rec)
            return True

    async def retry_step(
        self,
        job_id: str,
        worker_id: str,
        ready_at: datetime,
        failure: FailureInfo,
        now: datetime,
    ) -> bool:
        async with self.transaction() as tx:
            job = await self._owned_job(tx, job_id, worker_id)
            if job is None:
                return False
            if job.cancel_requested:
                await tx.execute("DELETE FROM lw_step_jobs WHERE job_id = ?", (job_id,))
                return True
            await tx.execute(
                "UPDATE lw_step_jobs SET lease_owner = NULL, lease_expires_at = NULL, "
                "ready_at = ?, last_failure = ? WHERE job_id = ?",
                (self.ts_in(ready_at), codec.failure_to_json(failure), job_id),
            )
            tx.notify(TOPIC_STEPS)
            return True

    async def renew_step_lease(self, job_id: str, worker_id: str, expires_at: datetime) -> bool:
        async with self.transaction() as tx:
            updated = await tx.execute(
                "UPDATE lw_step_jobs SET lease_expires_at = ? WHERE job_id = ? AND lease_owner = ?",
                (self.ts_in(expires_at), job_id, worker_id),
            )
        return updated > 0

    @staticmethod
    def _job_filters(
        sql: str,
        params: list[Any],
        step_names: Collection[str] | None,
        queues: Collection[str] | None,
    ) -> tuple[str, list[Any]]:
        for column, values in (("step_name", step_names), ("queue", queues)):
            if values is not None:
                clause, items = _in_clause(column, values)
                sql += f" AND {clause}"
                params.extend(items)
        return sql, params

    async def heartbeat_step(
        self,
        job_id: str,
        worker_id: str,
        expires_at: datetime,
        details: Payload | None,
        now: datetime,
    ) -> HeartbeatResult | None:
        async with self.transaction() as tx:
            job = await self._owned_job(tx, job_id, worker_id)
            if job is None:
                return None
            if details is None:
                await tx.execute(
                    "UPDATE lw_step_jobs SET lease_expires_at = ?, last_heartbeat_at = ? "
                    "WHERE job_id = ?",
                    (self.ts_in(expires_at), self.ts_in(now), job_id),
                )
            else:
                await tx.execute(
                    "UPDATE lw_step_jobs SET lease_expires_at = ?, last_heartbeat_at = ?, "
                    "heartbeat_details = ? WHERE job_id = ?",
                    (
                        self.ts_in(expires_at),
                        self.ts_in(now),
                        codec.payload_to_json(details),
                        job_id,
                    ),
                )
            return HeartbeatResult(cancel_requested=job.cancel_requested)

    async def release_step(
        self, job_id: str, worker_id: str, now: datetime, *, consume_attempt: bool
    ) -> bool:
        async with self.transaction() as tx:
            job = await self._owned_job(tx, job_id, worker_id)
            if job is None:
                return False
            if job.cancel_requested:
                await tx.execute("DELETE FROM lw_step_jobs WHERE job_id = ?", (job_id,))
                return True
            attempt = job.attempt if consume_attempt else max(job.attempt - 1, 0)
            await tx.execute(
                "UPDATE lw_step_jobs SET lease_owner = NULL, lease_expires_at = NULL, "
                "ready_at = ?, attempt = ? WHERE job_id = ?",
                (self.ts_in(min(job.ready_at, now)), attempt, job_id),
            )
            tx.notify(TOPIC_STEPS)
            return True

    async def next_step_ready_at(
        self,
        step_names: Collection[str] | None = None,
        queues: Collection[str] | None = None,
    ) -> datetime | None:
        if (step_names is not None and not step_names) or (queues is not None and not queues):
            return None
        sql = (
            "SELECT MIN(ready_at) AS next_at FROM lw_step_jobs "
            "WHERE lease_owner IS NULL AND cancel_requested = ?"
        )
        sql, params = self._job_filters(sql, [False], step_names, queues)
        async with self.transaction() as tx:
            row = await tx.fetchone(sql, params)
        return None if row is None else self.ts_out(row["next_at"])

    async def get_step_jobs(self, run_id: str) -> list[StepJob]:
        async with self.transaction() as tx:
            await self._load(tx, run_id, lock=False)
            rows = await tx.fetchall(
                "SELECT * FROM lw_step_jobs WHERE run_id = ? ORDER BY seq", (run_id,)
            )
        return [self._job(row) for row in rows]

    # ------------------------------------------------------------------ timers

    async def next_timer_at(self) -> datetime | None:
        async with self.transaction() as tx:
            row = await tx.fetchone("SELECT MIN(fire_at) AS next_at FROM lw_timers")
        return None if row is None else self.ts_out(row["next_at"])

    async def fire_due_timers(self, now: datetime, limit: int = 100) -> int:
        async with self.transaction() as tx:
            due = await tx.fetchall(
                "SELECT run_id, seq FROM lw_timers WHERE fire_at <= ? "
                "ORDER BY fire_at, run_id, seq LIMIT ?",
                (self.ts_in(now), limit),
            )
        fired = 0
        for row in due:
            # One short transaction per timer keeps the execution-then-timer lock order.
            async with self.transaction() as tx:
                rec = await self._load(tx, row["run_id"], lock=True)
                deleted = await tx.execute(
                    "DELETE FROM lw_timers WHERE run_id = ? AND seq = ?",
                    (row["run_id"], row["seq"]),
                )
                if not deleted:
                    continue  # fired or cancelled concurrently
                seq = int(row["seq"])
                if rec.is_closed:
                    pass
                elif seq == DEADLINE_TIMER_SEQ:
                    reason = "run exceeded its timeout"
                    await self._append(tx, rec, WorkflowTimedOut(reason=reason), now)
                    await self._close(
                        tx, rec, CloseOutcome(ExecutionStatus.TIMED_OUT, reason=reason), now
                    )
                    await self._write(tx, rec)
                else:
                    await self._append(tx, rec, TimerFired(seq=seq), now)
                    self._refresh_tick_pending(tx, rec, now)
                    await self._write(tx, rec)
                fired += 1
        return fired

    # ------------------------------------------------------------------ visibility

    async def _write_attributes(
        self, tx: SqlTransaction, run_id: str, encoded: dict[str, Any]
    ) -> None:
        for name, item in encoded.items():
            await tx.execute(
                "DELETE FROM lw_search_attributes WHERE run_id = ? AND name = ?", (run_id, name)
            )
            for value_type, column, value in attribute_rows(item):
                stored = self.ts_in(value) if isinstance(value, datetime) else value
                await tx.execute(
                    f"INSERT INTO lw_search_attributes (run_id, name, value_type, {column}) "
                    "VALUES (?, ?, ?, ?)",
                    (run_id, name, value_type, stored),
                )

    async def _inherit_attributes(
        self, tx: SqlTransaction, from_run: str, to_run: str, started: Event
    ) -> None:
        own = set(getattr(started, "search_attributes", {}) or {})
        rows = await tx.fetchall(
            "SELECT name, value_type, str_value, int_value, float_value, bool_value, time_value "
            "FROM lw_search_attributes WHERE run_id = ?",
            (from_run,),
        )
        for row in rows:
            if row["name"] in own:
                continue
            await tx.execute(
                "INSERT INTO lw_search_attributes (run_id, name, value_type, str_value, "
                "int_value, float_value, bool_value, time_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    to_run,
                    row["name"],
                    row["value_type"],
                    row["str_value"],
                    row["int_value"],
                    row["float_value"],
                    row["bool_value"],
                    row["time_value"],
                ),
            )

    async def query_executions(
        self, query: str, schema: SearchAttributes, now: datetime, limit: int = 100
    ) -> list[ExecutionRecord]:
        from loomwork.visibility import compile_sql, parse_query

        parsed = parse_query(query)
        compiled = compile_sql(
            parsed,
            schema,
            dialect=self.dialect,
            now=now,
            ts_in=self.ts_in,
            source=query,
        )
        order = compiled.order_by_sql or "seq DESC"
        effective = min(limit, parsed.limit) if parsed.limit is not None else limit
        sql = f"SELECT * FROM lw_executions WHERE {compiled.where_sql} " f"ORDER BY {order} LIMIT ?"
        async with self.transaction() as tx:
            rows = await tx.fetchall(sql, [*compiled.params, effective])
        return [self._record(row) for row in rows]

    async def get_search_attributes(self, run_id: str) -> dict[str, object]:
        async with self.transaction() as tx:
            await self._load(tx, run_id, lock=False)
            rows = await tx.fetchall(
                "SELECT * FROM lw_search_attributes WHERE run_id = ? ORDER BY name", (run_id,)
            )
        encoded: dict[str, Any] = {}
        for row in rows:
            value_type = row["value_type"]
            if value_type == "keyword":
                entry = encoded.setdefault(row["name"], {"type": "keyword_list", "value": []})
                entry["value"].append(row["str_value"])
                continue
            if value_type == "datetime":
                moment = self.ts_out(row["time_value"])
                value: Any = moment.isoformat() if moment is not None else None
            elif value_type == "bool":
                value = bool(row["bool_value"])
            else:
                value = row[
                    {"string": "str_value", "int": "int_value", "float": "float_value"}[value_type]
                ]
            encoded[row["name"]] = {"type": value_type, "value": value}
        return decode_attributes(encoded)

    # ------------------------------------------------------------------ outbox

    def _outbox_message(self, row: Row) -> OutboxMessage:
        event = None
        if row["event_kind"] is not None:
            body = dict(codec.loads(row["event_body"]) or {})
            body["kind"] = row["event_kind"]
            event = event_from_dict(body)
        return OutboxMessage(
            action=OutboxAction(row["action"]),
            target_run_id=row["target_run_id"],
            target_workflow_id=row["target_workflow_id"],
            event=event,
            reason=row["reason"] or "",
        )

    async def relay_outbox(self, now: datetime, limit: int = 100) -> int:
        async with self.transaction() as tx:
            ids = await tx.fetchall("SELECT id FROM lw_outbox ORDER BY id LIMIT ?", (limit,))
        processed = 0
        for id_row in ids:
            # One transaction per message: lock the message, then its single target run.
            async with self.transaction() as tx:
                row = await tx.fetchone(
                    f"SELECT * FROM lw_outbox WHERE id = ?{self.SKIP_LOCKED}", (id_row["id"],)
                )
                if row is None:
                    continue  # taken by a concurrent relay
                await tx.execute("DELETE FROM lw_outbox WHERE id = ?", (row["id"],))
                processed += 1
                message = self._outbox_message(row)
                target = await self._outbox_target(tx, message)
                if target is None or target.is_closed:
                    continue
                if message.action is OutboxAction.APPEND_INPUT:
                    assert message.event is not None
                    await self._append(tx, target, message.event, now)
                    self._refresh_tick_pending(tx, target, now)
                elif message.action is OutboxAction.REQUEST_CANCEL:
                    if not target.cancel_requested:
                        target.cancel_requested = True
                        await self._append(tx, target, CancelRequested(reason=message.reason), now)
                        self._refresh_tick_pending(tx, target, now)
                else:
                    await self._append(tx, target, WorkflowTerminated(reason=message.reason), now)
                    await self._close(
                        tx,
                        target,
                        CloseOutcome(ExecutionStatus.TERMINATED, reason=message.reason),
                        now,
                    )
                await self._write(tx, target)
        return processed

    async def _outbox_target(
        self, tx: SqlTransaction, message: OutboxMessage
    ) -> ExecutionRecord | None:
        if message.target_run_id is not None:
            row = await tx.fetchone(
                f"SELECT * FROM lw_executions WHERE run_id = ?{self.FOR_UPDATE}",
                (message.target_run_id,),
            )
        else:
            row = await tx.fetchone(
                "SELECT * FROM lw_executions WHERE workflow_id = ? AND status = ?"
                f"{self.FOR_UPDATE}",
                (message.target_workflow_id, ExecutionStatus.RUNNING.value),
            )
        return None if row is None else self._record(row)

    async def outbox_size(self) -> int:
        async with self.transaction() as tx:
            row = await tx.fetchone("SELECT COUNT(*) AS n FROM lw_outbox")
        return int(row["n"]) if row else 0

    async def list_children(self, run_id: str) -> list[ExecutionRecord]:
        async with self.transaction() as tx:
            await self._load(tx, run_id, lock=False)
            rows = await tx.fetchall(
                "SELECT * FROM lw_executions WHERE parent_run_id = ? ORDER BY seq", (run_id,)
            )
        return [self._record(row) for row in rows]

    # ------------------------------------------------------------------ schedules

    def _schedule(self, row: Row) -> ScheduleState:
        return ScheduleState.from_dict(codec.loads(row["body"])).with_(version=int(row["version"]))

    def _schedule_params(self, state: ScheduleState, now: datetime) -> tuple[Any, ...]:
        return (
            self.ts_in(state.next_fire_at),
            state.paused,
            state.buffered is not None,
            state.version,
            codec.dumps(state.to_dict()),
            self.ts_in(now),
        )

    async def create_schedule(self, state: ScheduleState, now: datetime) -> ScheduleState:
        stored = state.with_(version=1, created_at=now, updated_at=now)
        try:
            async with self.transaction() as tx:
                if await tx.fetchone(
                    "SELECT 1 FROM lw_schedules WHERE schedule_id = ?", (state.schedule_id,)
                ):
                    raise ScheduleExistsError(f"schedule {state.schedule_id!r} already exists")
                await tx.execute(
                    "INSERT INTO lw_schedules (next_fire_at, paused, has_buffer, version, body, "
                    "updated_at, schedule_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (*self._schedule_params(stored, now), stored.schedule_id),
                )
                tx.notify(TOPIC_SCHEDULES)
        except ScheduleExistsError:
            raise
        except Exception as exc:
            if self.is_unique_violation(exc):
                raise ScheduleExistsError(f"schedule {state.schedule_id!r} already exists") from exc
            raise
        return stored

    async def get_schedule(self, schedule_id: str) -> ScheduleState:
        async with self.transaction() as tx:
            row = await tx.fetchone(
                "SELECT version, body FROM lw_schedules WHERE schedule_id = ?", (schedule_id,)
            )
        if row is None:
            raise NotFoundError(f"schedule {schedule_id!r} does not exist")
        return self._schedule(row)

    async def list_schedules(self, limit: int = 100) -> list[ScheduleState]:
        async with self.transaction() as tx:
            rows = await tx.fetchall(
                "SELECT version, body FROM lw_schedules ORDER BY schedule_id LIMIT ?", (limit,)
            )
        return [self._schedule(row) for row in rows]

    async def _lock_schedule(
        self, tx: SqlTransaction, schedule_id: str, expected_version: int
    ) -> None:
        row = await tx.fetchone(
            f"SELECT version FROM lw_schedules WHERE schedule_id = ?{self.FOR_UPDATE}",
            (schedule_id,),
        )
        if row is None:
            raise NotFoundError(f"schedule {schedule_id!r} does not exist")
        if int(row["version"]) != expected_version:
            raise ScheduleConflictError(
                f"schedule {schedule_id!r} is at version {row['version']}, "
                f"expected {expected_version}"
            )

    async def _save_schedule(self, tx: SqlTransaction, state: ScheduleState, now: datetime) -> None:
        await tx.execute(
            "UPDATE lw_schedules SET next_fire_at = ?, paused = ?, has_buffer = ?, version = ?, "
            "body = ?, updated_at = ? WHERE schedule_id = ?",
            (*self._schedule_params(state, now), state.schedule_id),
        )
        tx.notify(TOPIC_SCHEDULES)

    async def update_schedule(
        self, state: ScheduleState, expected_version: int, now: datetime
    ) -> ScheduleState:
        stored = state.with_(version=expected_version + 1, updated_at=now)
        async with self.transaction() as tx:
            await self._lock_schedule(tx, state.schedule_id, expected_version)
            await self._save_schedule(tx, stored, now)
        return stored

    async def delete_schedule(self, schedule_id: str) -> bool:
        async with self.transaction() as tx:
            deleted = await tx.execute(
                "DELETE FROM lw_schedules WHERE schedule_id = ?", (schedule_id,)
            )
        return bool(deleted)

    async def due_schedules(self, now: datetime, limit: int = 100) -> list[ScheduleState]:
        async with self.transaction() as tx:
            rows = await tx.fetchall(
                "SELECT version, body FROM lw_schedules WHERE has_buffer = ? "
                "OR (paused = ? AND next_fire_at <= ?) ORDER BY schedule_id LIMIT ?",
                (True, False, self.ts_in(now), limit),
            )
        return [self._schedule(row) for row in rows]

    async def fire_schedule(
        self,
        state: ScheduleState,
        expected_version: int,
        starts: Sequence[ChildSpec],
        outbox: Sequence[OutboxMessage],
        now: datetime,
    ) -> ScheduleState:
        async with self.transaction() as tx:
            await self._lock_schedule(tx, state.schedule_id, expected_version)
            conflicts: set[str] = set()
            for spec in starts:
                running = await tx.fetchone(
                    "SELECT 1 FROM lw_executions WHERE (workflow_id = ? AND status = ?) "
                    "OR run_id = ?",
                    (spec.record.workflow_id, ExecutionStatus.RUNNING.value, spec.record.run_id),
                )
                if running is not None:
                    conflicts.add(spec.record.run_id)
                    continue
                await self._insert_run(tx, dataclasses.replace(spec.record), spec.started, now)
            await self._enqueue(tx, outbox, now)
            stored = drop_unstarted(state, conflicts).with_(
                version=expected_version + 1, updated_at=now
            )
            await self._save_schedule(tx, stored, now)
        return stored

    async def next_schedule_at(self) -> datetime | None:
        async with self.transaction() as tx:
            row = await tx.fetchone(
                "SELECT MIN(next_fire_at) AS next_at FROM lw_schedules WHERE paused = ?", (False,)
            )
        return None if row is None else self.ts_out(row["next_at"])

    # ------------------------------------------------------------------ maintenance

    async def delete_closed_before(self, cutoff: datetime, limit: int = 1000) -> int:
        """Delete histories of runs closed before ``cutoff``. Returns runs removed."""
        async with self.transaction() as tx:
            rows = await tx.fetchall(
                "SELECT run_id FROM lw_executions WHERE status <> ? AND closed_at < ? "
                "ORDER BY closed_at LIMIT ?",
                (ExecutionStatus.RUNNING.value, self.ts_in(cutoff), limit),
            )
            for row in rows:
                await tx.execute("DELETE FROM lw_events WHERE run_id = ?", (row["run_id"],))
                await tx.execute(
                    "DELETE FROM lw_search_attributes WHERE run_id = ?", (row["run_id"],)
                )
                await tx.execute("DELETE FROM lw_executions WHERE run_id = ?", (row["run_id"],))
        return len(rows)


__all__ = ["SQLStore", "SqlTransaction"]
