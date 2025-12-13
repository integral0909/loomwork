"""The storage contract every backend implements.

Every method is **atomic**: it either applies all of its effects or none. Callers pass
``now`` explicitly so that stores never read a clock themselves; this keeps behaviour
reproducible under a :class:`~loomwork.clock.ManualClock`.

The conformance suite in ``tests/store_conformance.py`` specifies the behaviour in
detail and is run against every backend.
"""

from __future__ import annotations

import abc
from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event, StepCompleted, StepFailed, WorkflowStarted
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint
from loomwork.schedules.model import ScheduleState

if TYPE_CHECKING:
    from loomwork.visibility.schema import SearchAttributes
from loomwork.store.notify import Notifier
from loomwork.store.records import (
    BuildCompatibility,
    ChildSpec,
    ExecutionRecord,
    ExecutionStatus,
    HeartbeatResult,
    OutboxMessage,
    StepJob,
    TickOutcome,
    WorkerPresence,
)

#: Statuses that satisfy IdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY: the latest run did
#: not complete successfully. CONTINUED_AS_NEW is deliberately excluded even though it is
#: not a failure either: in practice it is never the *latest* run of a workflow id (its
#: successor, created in the same transaction, immediately takes that place), and
#: treating it as reusable would let a caller race a continuation still being written.
_REUSE_FAILED_ONLY_STATUSES = frozenset(
    {
        ExecutionStatus.FAILED,
        ExecutionStatus.CANCELED,
        ExecutionStatus.TERMINATED,
        ExecutionStatus.TIMED_OUT,
    }
)


def id_reuse_allowed(policy: IdReusePolicy, latest_status: ExecutionStatus) -> bool:
    """Whether ``policy`` allows starting a new run given the workflow id's latest
    (closed) run status. Shared by every store so the rule is identical everywhere."""
    if policy is IdReusePolicy.ALLOW_DUPLICATE:
        return True
    if policy is IdReusePolicy.REJECT_DUPLICATE:
        return False
    return latest_status in _REUSE_FAILED_ONLY_STATUSES


class Store(abc.ABC):
    """Persistence for histories, executions, step jobs and timers."""

    def __init__(self) -> None:
        self.notifier = Notifier()

    # ------------------------------------------------------------------ lifecycle

    async def open(self) -> None:  # noqa: B027 - optional hook
        """Prepare the store for use (connect, run migrations, ...)."""

    async def close(self) -> None:  # noqa: B027 - optional hook
        """Release resources held by the store."""

    # ------------------------------------------------------------------ executions

    @abc.abstractmethod
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
        """Insert a new running execution whose history starts with ``started``,
        followed by ``extra_inputs`` (used by signal-with-start).

        The workflow id's existing runs, if any, are checked and acted on atomically in
        the same transaction that inserts the new row, so two concurrent callers can
        never both believe they won a ``TERMINATE_EXISTING`` race or both see "no
        running execution" for ``REJECT_DUPLICATE``:

        * If a run of ``record.workflow_id`` is currently running, ``id_conflict_policy``
          decides: ``FAIL`` raises :class:`~loomwork.errors.WorkflowAlreadyStartedError`;
          ``USE_EXISTING`` does not create a new run and instead returns the running
          execution's current record (paired with the given ``started``, which is not
          persisted); ``TERMINATE_EXISTING`` terminates it (recorded reason: "superseded
          by id conflict policy") and proceeds to create the new run in the same
          transaction.
        * Otherwise, if the workflow id has at least one previous (closed) run,
          ``id_reuse_policy`` decides: ``ALLOW_DUPLICATE`` always proceeds;
          ``ALLOW_DUPLICATE_FAILED_ONLY`` proceeds only if the latest run did not
          complete successfully; ``REJECT_DUPLICATE`` never proceeds. Either rejecting
          case raises :class:`~loomwork.errors.WorkflowIdReuseRejectedError`.

        Raises:
            WorkflowAlreadyStartedError: ``id_conflict_policy`` is ``FAIL`` (the
                default) and another run with the same workflow id is still running.
            WorkflowIdReuseRejectedError: ``id_reuse_policy`` forbids starting a new
                run given the workflow id's latest (closed) run.
        """

    @abc.abstractmethod
    async def get_execution(self, run_id: str) -> ExecutionRecord:
        """Raises :class:`~loomwork.errors.NotFoundError` if the run does not exist."""

    @abc.abstractmethod
    async def find_execution(self, workflow_id: str) -> ExecutionRecord:
        """The most recently created run for ``workflow_id``.

        Raises :class:`~loomwork.errors.NotFoundError` if there is none.
        """

    @abc.abstractmethod
    async def list_executions(
        self,
        *,
        workflow_name: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        """Executions ordered from newest to oldest."""

    # ------------------------------------------------------------------ history

    @abc.abstractmethod
    async def load_history(
        self, run_id: str, after_id: int = 0, limit: int | None = None
    ) -> list[Event]:
        """Events with ``event_id > after_id`` in ascending order."""

    @abc.abstractmethod
    async def append_input(self, run_id: str, event: Event, now: datetime) -> Event | None:
        """Append an input event and mark a tick as pending.

        Returns the stored event, or ``None`` if the run is already closed.
        """

    @abc.abstractmethod
    async def request_cancel(self, run_id: str, reason: str, now: datetime) -> bool:
        """Append a ``CancelRequested`` input once. Returns ``False`` if the run is closed."""

    @abc.abstractmethod
    async def terminate(self, run_id: str, reason: str, now: datetime) -> bool:
        """Close the run immediately without involving workflow code.

        Returns ``False`` if the run was already closed.
        """

    @abc.abstractmethod
    async def reset_run(
        self,
        run_id: str,
        new_run_id: str,
        to: ResetPoint,
        reason: str,
        now: datetime,
    ) -> tuple[ExecutionRecord, ExecutionRecord]:
        """Start ``new_run_id`` as ``run_id``'s history truncated at ``to``.

        Atomically, in one transaction per run touched:

        1. If ``run_id`` is still running, it is terminated (``reason`` recorded, and
           its ``continued_run_id`` set to ``new_run_id``); step jobs and timers of
           ``run_id`` are deleted as part of that close, exactly as any other
           termination. If it was already closed, only ``continued_run_id`` is set
           (its status, result and failure are untouched).
        2. A new execution ``new_run_id`` is created for the same workflow id, whose
           history is a byte-for-byte prefix of ``run_id``'s history: every event up to
           and including the tick boundary ``to`` resolves to (event ids are preserved).
           ``new_run_id.reset_from_run_id`` is set to ``run_id``.
        3. Every *input* event of ``run_id`` after that boundary (signals, updates,
           step or timer outcomes that already arrived, ...) is re-delivered to
           ``new_run_id`` as a freshly appended input, so nothing sent to the old run is
           lost. Decision events after the boundary are dropped: that is the point of
           resetting.
        4. Steps and timers that ``to`` leaves outstanding (a step scheduled by a kept
           tick with no matching outcome in the kept history, likewise for timers) are
           recreated for ``new_run_id`` so live processing can complete them; none of
           ``run_id``'s step jobs or timers are copied or reused.

        Returns ``(old_record, new_record)`` after the transaction(s).

        Raises:
            NotFoundError: ``run_id`` does not exist.
            InvalidResetPointError: ``to`` does not resolve against ``run_id``'s history
                (for example, :meth:`~loomwork.options.ResetPoint.after_tick` with a
                count higher than the number of ticks the run completed).
        """

    # ------------------------------------------------------------------ ticks

    @abc.abstractmethod
    async def claim_tick(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        workflow_names: Collection[str] | None = None,
        build_id: str | None = None,
    ) -> ExecutionRecord | None:
        """Lease a running execution with a pending, ready tick.

        Executions whose lease has expired are eligible again. Among eligible executions
        the one that has been ready the longest is chosen.

        ``build_id`` is this worker's declared build (see :class:`~loomwork.worker.
        worker.WorkerOptions`). ``None`` means the worker is unversioned: it is eligible
        for every execution, pinned or not (today's behaviour, unchanged). A worker that
        declares a build is only eligible for an execution whose ``pinned_build_id`` is
        ``None`` (not yet pinned), equal to ``build_id``, or declared compatible with
        ``build_id`` (see :meth:`declare_build_compatible`); see
        ``docs/adr/0006-worker-build-versioning.md``.
        """

    @abc.abstractmethod
    async def complete_tick(
        self, run_id: str, worker_id: str, outcome: TickOutcome, now: datetime
    ) -> list[Event]:
        """Commit a tick and release the lease. Returns the appended events.

        A ``TickCompleted`` event is written unless the tick consumed no new input and
        produced no decision. Step jobs and timers are created or cancelled as described
        by ``outcome``; a closing outcome deletes every remaining job and timer of the
        run. Child executions in ``outcome.new_children`` are created in the same
        transaction; a child whose workflow id is already running is not created and
        the parent receives a ``ChildFailed`` input instead. Outbox messages are stored
        for the relay.

        Closing a run (here or through :meth:`terminate`) also enqueues outbox messages:
        the run's result to its parent, and its parent-close policy to running children.

        Raises:
            LeaseLostError: ``worker_id`` no longer holds the lease, or the run was
                closed (for example terminated) while the tick was running.
        """

    @abc.abstractmethod
    async def fail_tick(
        self,
        run_id: str,
        worker_id: str,
        error: str,
        retry_at: datetime,
        now: datetime,
    ) -> None:
        """Release the lease after a failed tick and schedule a retry at ``retry_at``."""

    # ------------------------------------------------------------------ steps

    @abc.abstractmethod
    async def claim_step(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        step_names: Collection[str] | None = None,
        queues: Collection[str] | None = None,
        build_id: str | None = None,
    ) -> StepJob | None:
        """Lease a ready step job and increment its attempt counter.

        Only jobs whose step name is in ``step_names`` and whose queue is in ``queues``
        are considered (``None`` means no restriction). Higher priorities are claimed
        first, then older ``ready_at``.

        ``build_id`` filters exactly as in :meth:`claim_tick`, against the job's own
        ``pinned_build_id`` (set only for queues opted into pinning; see
        :attr:`~loomwork.engine.config.EngineConfig.pinned_queues`).
        """

    @abc.abstractmethod
    async def finish_step(
        self,
        job_id: str,
        worker_id: str,
        event: StepCompleted | StepFailed,
        now: datetime,
    ) -> bool:
        """Delete the job and append its outcome to the run's history.

        Returns ``False`` if the job no longer exists or is leased by someone else; the
        outcome is then discarded.
        """

    @abc.abstractmethod
    async def retry_step(
        self,
        job_id: str,
        worker_id: str,
        ready_at: datetime,
        failure: FailureInfo,
        now: datetime,
    ) -> bool:
        """Release the job for another attempt no earlier than ``ready_at``."""

    @abc.abstractmethod
    async def renew_step_lease(self, job_id: str, worker_id: str, expires_at: datetime) -> bool:
        """Set the lease expiry of a job this worker holds (heartbeats, or tightening the
        claim-time lease to the step's own timeout). ``False`` if the lease was lost."""

    @abc.abstractmethod
    async def heartbeat_step(
        self,
        job_id: str,
        worker_id: str,
        expires_at: datetime,
        details: Payload | None,
        now: datetime,
    ) -> HeartbeatResult | None:
        """Record a heartbeat: renew the lease and store progress details.

        Returns ``None`` if the worker no longer holds the lease.
        """

    @abc.abstractmethod
    async def release_step(
        self, job_id: str, worker_id: str, now: datetime, *, consume_attempt: bool
    ) -> bool:
        """Give up a claimed job so that it can be claimed again immediately.

        Used on graceful shutdown (``consume_attempt=False`` undoes the claim's attempt
        increment) and when an attempt was cancelled. A job whose workflow requested
        cancellation is deleted instead.
        """

    @abc.abstractmethod
    async def next_step_ready_at(
        self,
        step_names: Collection[str] | None = None,
        queues: Collection[str] | None = None,
    ) -> datetime | None:
        """Earliest ``ready_at`` among unleased jobs matching the filters."""

    @abc.abstractmethod
    async def get_step_jobs(self, run_id: str) -> list[StepJob]:
        """Outstanding step jobs of a run, ordered by sequence number."""

    # ------------------------------------------------------------------ worker builds

    @abc.abstractmethod
    async def repin_run(self, run_id: str, build_id: str | None, now: datetime) -> ExecutionRecord:
        """Set ``run_id``'s ``pinned_build_id`` to ``build_id`` (``None`` unpins it, so
        the next tick pins to whichever build processes it, exactly as an unpinned run
        that has never ticked).

        Moves a stuck run off a build with no live workers, or onto one made compatible
        after the fact. Does not touch the run's ``versioning_policy``: a run repinned
        while ``AUTO_UPGRADE`` accepts the new value but it has no effect on claiming
        until the run is also switched to ``PINNED``.

        Raises:
            NotFoundError: ``run_id`` does not exist.
            WorkflowClosedError: ``run_id`` has already closed.
        """

    @abc.abstractmethod
    async def declare_build_compatible(
        self, build_id: str, compatible_with: str, now: datetime
    ) -> None:
        """Declare that workers of ``build_id`` may claim ticks and pinned-queue steps
        pinned to ``compatible_with`` (directed; the reverse is not implied). Idempotent."""

    @abc.abstractmethod
    async def revoke_build_compatibility(self, build_id: str, compatible_with: str) -> bool:
        """Undo :meth:`declare_build_compatible`. Returns ``False`` if it was not set."""

    @abc.abstractmethod
    async def list_build_compatibility(self) -> list[BuildCompatibility]:
        """Every declared compatibility, newest first."""

    @abc.abstractmethod
    async def heartbeat_worker(
        self, worker_id: str, build_id: str, now: datetime, expires_at: datetime
    ) -> None:
        """Record that ``worker_id`` (running ``build_id``) is alive until ``expires_at``.

        Cheap by design: one upsert on a primary key, called at a low frequency (see
        :attr:`~loomwork.worker.worker.WorkerOptions.build_heartbeat_interval`) --
        presence is only used to warn operators, never to gate claiming.
        """

    @abc.abstractmethod
    async def list_worker_builds(self, now: datetime) -> list[WorkerPresence]:
        """Live worker presence rows (``expires_at > now``), most recent first."""

    @abc.abstractmethod
    async def count_pinned_runs(self) -> dict[str, int]:
        """Number of currently running executions pinned to each build id.

        Excludes runs whose policy is ``PINNED`` but that have not ticked yet (their
        ``pinned_build_id`` is still ``None``). Used to tell operators which builds still
        have work and are therefore not yet safe to retire.
        """

    # ------------------------------------------------------------------ visibility

    @abc.abstractmethod
    async def query_executions(
        self, query: str, schema: SearchAttributes, now: datetime, limit: int = 100
    ) -> list[ExecutionRecord]:
        """Executions matching a visibility query (see ``docs/visibility-queries.md``).

        Without an ``ORDER BY`` clause results are newest first. ``limit`` caps the result
        size (a ``LIMIT`` in the query may lower it further).

        Raises:
            QuerySyntaxError, QueryTypeError: the query is invalid.
        """

    @abc.abstractmethod
    async def get_search_attributes(self, run_id: str) -> dict[str, object]:
        """Current search attributes of a run, decoded to Python values."""

    # ------------------------------------------------------------------ outbox

    @abc.abstractmethod
    async def relay_outbox(self, now: datetime, limit: int = 100) -> int:
        """Apply up to ``limit`` outbox messages, each in its own transaction.

        Messages whose target does not exist or has closed are discarded. Returns the
        number of messages processed.
        """

    @abc.abstractmethod
    async def outbox_size(self) -> int:
        """Number of messages waiting for the relay."""

    @abc.abstractmethod
    async def list_children(self, run_id: str) -> list[ExecutionRecord]:
        """Executions started as children of ``run_id``, oldest first."""

    # ------------------------------------------------------------------ schedules

    @abc.abstractmethod
    async def create_schedule(self, state: ScheduleState, now: datetime) -> ScheduleState:
        """Store a new schedule (its version becomes 1).

        Raises:
            ScheduleExistsError: the schedule id is taken.
        """

    @abc.abstractmethod
    async def get_schedule(self, schedule_id: str) -> ScheduleState:
        """Raises :class:`~loomwork.errors.NotFoundError` if it does not exist."""

    @abc.abstractmethod
    async def list_schedules(self, limit: int = 100) -> list[ScheduleState]:
        """Schedules ordered by id."""

    @abc.abstractmethod
    async def update_schedule(
        self, state: ScheduleState, expected_version: int, now: datetime
    ) -> ScheduleState:
        """Replace a schedule if its stored version is ``expected_version``.

        Raises:
            ScheduleConflictError: it was changed concurrently.
            NotFoundError: it does not exist.
        """

    @abc.abstractmethod
    async def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a schedule (runs it started are unaffected)."""

    @abc.abstractmethod
    async def due_schedules(self, now: datetime, limit: int = 100) -> list[ScheduleState]:
        """Schedules to evaluate: next fire time reached, or a buffered fire pending."""

    @abc.abstractmethod
    async def fire_schedule(
        self,
        state: ScheduleState,
        expected_version: int,
        starts: Sequence[ChildSpec],
        outbox: Sequence[OutboxMessage],
        now: datetime,
    ) -> ScheduleState:
        """Atomically save ``state``, create the runs in ``starts`` and enqueue ``outbox``.

        A start whose workflow id is already running is not created; the corresponding
        entry is removed from ``state.running`` and recorded as ``already_running``.

        Raises:
            ScheduleConflictError: the schedule changed concurrently; nothing is applied.
        """

    @abc.abstractmethod
    async def next_schedule_at(self) -> datetime | None:
        """Earliest ``next_fire_at`` of any unpaused schedule."""

    # ------------------------------------------------------------------ retention

    @abc.abstractmethod
    async def delete_closed_before(self, cutoff: datetime, limit: int = 1000) -> int:
        """Delete up to ``limit`` runs (record and history) closed before ``cutoff``.

        Returns the number of runs deleted. Running executions are never touched.
        """

    # ------------------------------------------------------------------ timers

    @abc.abstractmethod
    async def next_timer_at(self) -> datetime | None:
        """Fire time of the earliest outstanding timer."""

    @abc.abstractmethod
    async def fire_due_timers(self, now: datetime, limit: int = 100) -> int:
        """Append ``TimerFired`` for up to ``limit`` timers due at ``now``.

        Returns the number of timers fired.
        """
