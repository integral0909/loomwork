"""The storage contract every backend implements.

Every method is **atomic**: it either applies all of its effects or none. Callers pass
``now`` explicitly so that stores never read a clock themselves; this keeps behaviour
reproducible under a :class:`~loomwork.clock.ManualClock`.

The conformance suite in ``tests/store_conformance.py`` specifies the behaviour in
detail and is run against every backend.
"""

from __future__ import annotations

import abc
from collections.abc import Collection
from datetime import datetime, timedelta

from loomwork.failure import FailureInfo
from loomwork.history.events import Event, StepCompleted, StepFailed, WorkflowStarted
from loomwork.store.notify import Notifier
from loomwork.store.records import (
    ExecutionRecord,
    ExecutionStatus,
    StepJob,
    TickOutcome,
)


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
        self, record: ExecutionRecord, started: WorkflowStarted, now: datetime
    ) -> tuple[ExecutionRecord, Event]:
        """Insert a new running execution whose history starts with ``started``.

        Raises:
            WorkflowAlreadyStartedError: another run with the same workflow id is
                still running.
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

    # ------------------------------------------------------------------ ticks

    @abc.abstractmethod
    async def claim_tick(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        workflow_names: Collection[str] | None = None,
    ) -> ExecutionRecord | None:
        """Lease a running execution with a pending, ready tick.

        Executions whose lease has expired are eligible again. Among eligible executions
        the one that has been ready the longest is chosen.
        """

    @abc.abstractmethod
    async def complete_tick(
        self, run_id: str, worker_id: str, outcome: TickOutcome, now: datetime
    ) -> list[Event]:
        """Commit a tick and release the lease. Returns the appended events.

        A ``TickCompleted`` event is written unless the tick consumed no new input and
        produced no decision. Step jobs and timers are created or cancelled as described
        by ``outcome``; a closing outcome deletes every remaining job and timer of the
        run.

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
    ) -> StepJob | None:
        """Lease a ready step job and increment its attempt counter."""

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
    async def extend_step_lease(self, job_id: str, worker_id: str, expires_at: datetime) -> bool:
        """Extend the lease on a job this worker holds. ``False`` if it was lost."""

    @abc.abstractmethod
    async def get_step_jobs(self, run_id: str) -> list[StepJob]:
        """Outstanding step jobs of a run, ordered by sequence number."""

    # ------------------------------------------------------------------ timers

    @abc.abstractmethod
    async def next_timer_at(self) -> datetime | None:
        """Fire time of the earliest outstanding timer."""

    @abc.abstractmethod
    async def fire_due_timers(self, now: datetime, limit: int = 100) -> int:
        """Append ``TimerFired`` for up to ``limit`` timers due at ``now``.

        Returns the number of timers fired.
        """
