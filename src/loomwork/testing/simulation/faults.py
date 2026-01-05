"""A store wrapper that lies about outcomes on a seeded, reproducible schedule.

:class:`FaultyStore` delegates every call to a real :class:`~loomwork.store.base.Store`
(:class:`~loomwork.store.memory.MemoryStore` or a SQL store) and never changes what that
store actually does -- it only sometimes raises instead of returning normally, in one of
two ways:

* **before**: the call never reaches the inner store. Nothing happened.
* **after**: the call reaches the inner store and completes -- the transaction commits,
  the notification fires -- and *then* a fault is raised to the caller anyway. This is
  the hardest case a real deployment has to tolerate: a connection drop between a
  commit and its acknowledgement. The engine cannot tell this apart from a call that
  never happened and must be able to recover regardless (idempotent retries, lease
  ownership checks, at-least-once step execution).

It also injects **latency** (a small real delay before the call, seeded, to perturb
scheduling and widen the set of interleavings a worker sees) and **lease-expiry
pressure** (shrinking the lease duration a worker asked for on ``claim_tick``/
``claim_step``, so leases expire almost immediately and the "only the lease holder
commits" invariant gets exercised hard).

Every fault is drawn from a :class:`random.Random` seeded once at construction, so two
:class:`FaultyStore` instances built with the same seed and :class:`FaultConfig` inject
exactly the same sequence of faults, in the same order, provided the calling code issues
its store calls in the same order -- which is what makes a failing simulation seed
reproducible.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypeVar

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import Event, StepCompleted, StepFailed, WorkflowStarted
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint
from loomwork.schedules.model import ScheduleState
from loomwork.store.base import Store
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

if TYPE_CHECKING:
    from loomwork.visibility.schema import SearchAttributes

_R = TypeVar("_R")

__all__ = [
    "MUTATING_METHODS",
    "FaultConfig",
    "FaultEvent",
    "FaultRates",
    "FaultyStore",
    "InjectedFault",
]


class InjectedFault(ConnectionError):
    """Raised by :class:`FaultyStore` in place of a real transport or database failure."""


def _fraction(value: float, name: str) -> float:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be within [0, 1], got {value!r}")
    return value


@dataclass(frozen=True, slots=True)
class FaultRates:
    """Failure probabilities for one store operation, each independently rolled."""

    before: float = 0.0
    after: float = 0.0
    latency: float = 0.0

    def __post_init__(self) -> None:
        _fraction(self.before, "before")
        _fraction(self.after, "after")
        _fraction(self.latency, "latency")


#: Operations that change store state or hand out leases -- the ones a real backend
#: failure can plausibly hit. Pure reads (``get_execution``, ``load_history``,
#: ``list_executions``, ...) are left alone: callers, including this harness's own
#: invariant checker, need a reliable way to observe the store.
MUTATING_METHODS: frozenset[str] = frozenset(
    {
        "create_execution",
        "append_input",
        "request_cancel",
        "terminate",
        "reset_run",
        "claim_tick",
        "complete_tick",
        "fail_tick",
        "claim_step",
        "finish_step",
        "retry_step",
        "renew_step_lease",
        "heartbeat_step",
        "release_step",
        "relay_outbox",
        "fire_due_timers",
        "create_schedule",
        "update_schedule",
        "delete_schedule",
        "fire_schedule",
        "delete_closed_before",
        "repin_run",
        "declare_build_compatible",
        "revoke_build_compatibility",
        "heartbeat_worker",
    }
)

#: Operations whose ``lease`` argument :attr:`FaultConfig.lease_pressure` may shrink.
LEASE_METHODS: frozenset[str] = frozenset({"claim_tick", "claim_step"})


@dataclass(frozen=True, slots=True)
class FaultConfig:
    """The seeded fault schedule a :class:`FaultyStore` draws from.

    Attributes:
        default: rates applied to every operation in ``methods`` without its own entry
            in ``overrides``.
        overrides: per-operation rates, keyed by the :class:`~loomwork.store.base.Store`
            method name (for example ``"complete_tick"``).
        methods: operations eligible for fault injection at all; defaults to
            :data:`MUTATING_METHODS`.
        lease_pressure: probability that ``claim_tick``/``claim_step`` receive
            :attr:`pressured_lease` instead of the lease duration the caller asked for.
        pressured_lease: the shrunk lease duration used under lease-expiry pressure.
        max_latency: upper bound, in seconds, of the real delay a "latency" fault adds.
    """

    default: FaultRates = field(default_factory=FaultRates)
    overrides: Mapping[str, FaultRates] = field(default_factory=dict)
    methods: frozenset[str] = MUTATING_METHODS
    lease_pressure: float = 0.0
    pressured_lease: timedelta = timedelta(milliseconds=1)
    max_latency: float = 0.005

    def __post_init__(self) -> None:
        _fraction(self.lease_pressure, "lease_pressure")
        if self.pressured_lease <= timedelta(0):
            raise ValueError("pressured_lease must be positive")
        if self.max_latency < 0:
            raise ValueError("max_latency must be >= 0")

    def rates_for(self, name: str) -> FaultRates:
        if name not in self.methods:
            return FaultRates()
        return self.overrides.get(name, self.default)

    @classmethod
    def uniform(cls, rate: float, *, lease_pressure: float = 0.0) -> FaultConfig:
        """A convenience config: ``rate`` split evenly across before/after/latency,
        applied to every mutating operation."""
        share = rate / 3
        return cls(
            default=FaultRates(before=share, after=share, latency=share),
            lease_pressure=lease_pressure,
        )


@dataclass(frozen=True, slots=True)
class FaultEvent:
    """One fault the schedule actually injected, kept for the simulation trace."""

    seq: int
    operation: str
    kind: str  # "before" | "after" | "latency" | "lease_pressure"

    def __str__(self) -> str:
        return f"#{self.seq} {self.kind}({self.operation})"


class FaultyStore(Store):
    """Delegates to ``inner``, injecting seeded faults around its mutating operations.

    Never corrupts ``inner``: every call either reaches it unchanged or does not reach
    it at all. Shares ``inner``'s :class:`~loomwork.store.notify.Notifier` so that
    workers built against this wrapper see the same wake-ups they would against
    ``inner`` directly -- a "before" fault suppresses the notification that call would
    have published (nothing happened), an "after" fault does not (it did).
    """

    def __init__(self, inner: Store, *, seed: int, config: FaultConfig | None = None) -> None:
        super().__init__()
        self.notifier = inner.notifier
        self._inner = inner
        self._rng = random.Random(seed)
        self.config = config or FaultConfig()
        self.injected: list[FaultEvent] = []
        self._seq = 0

    # ------------------------------------------------------------------ fault schedule

    async def _call(self, name: str, action: Callable[[], Awaitable[_R]]) -> _R:
        rates = self.config.rates_for(name)
        if rates.before and self._rng.random() < rates.before:
            self._inject(name, "before")
            raise InjectedFault(f"{name} failed before reaching the store")
        if rates.latency and self._rng.random() < rates.latency:
            self._inject(name, "latency")
            await asyncio.sleep(self._rng.uniform(0, self.config.max_latency))
        result = await action()
        # A fresh roll: whether the *reply* is lost is independent of whether the call
        # was itself delayed or clean -- by this point the operation already committed.
        if rates.after and self._rng.random() < rates.after:
            self._inject(name, "after")
            raise InjectedFault(f"{name} committed but its reply was lost")
        return result

    def _inject(self, operation: str, kind: str) -> None:
        self._seq += 1
        self.injected.append(FaultEvent(self._seq, operation, kind))

    def _leased(self, name: str, lease: timedelta) -> timedelta:
        if self.config.lease_pressure and self._rng.random() < self.config.lease_pressure:
            self._inject(name, "lease_pressure")
            return self.config.pressured_lease
        return lease

    # ------------------------------------------------------------------ lifecycle

    async def open(self) -> None:
        await self._inner.open()

    async def close(self) -> None:
        await self._inner.close()

    # ------------------------------------------------------------------ executions (reads)

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
        return await self._call(
            "create_execution",
            lambda: self._inner.create_execution(
                record,
                started,
                now,
                extra_inputs,
                id_reuse_policy=id_reuse_policy,
                id_conflict_policy=id_conflict_policy,
            ),
        )

    async def get_execution(self, run_id: str) -> ExecutionRecord:
        return await self._inner.get_execution(run_id)

    async def find_execution(self, workflow_id: str) -> ExecutionRecord:
        return await self._inner.find_execution(workflow_id)

    async def list_executions(
        self,
        *,
        workflow_name: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        return await self._inner.list_executions(
            workflow_name=workflow_name, status=status, limit=limit
        )

    # ------------------------------------------------------------------ history

    async def load_history(
        self, run_id: str, after_id: int = 0, limit: int | None = None
    ) -> list[Event]:
        return await self._inner.load_history(run_id, after_id, limit)

    async def append_input(self, run_id: str, event: Event, now: datetime) -> Event | None:
        return await self._call(
            "append_input", lambda: self._inner.append_input(run_id, event, now)
        )

    async def request_cancel(self, run_id: str, reason: str, now: datetime) -> bool:
        return await self._call(
            "request_cancel", lambda: self._inner.request_cancel(run_id, reason, now)
        )

    async def terminate(self, run_id: str, reason: str, now: datetime) -> bool:
        return await self._call("terminate", lambda: self._inner.terminate(run_id, reason, now))

    async def reset_run(
        self,
        run_id: str,
        new_run_id: str,
        to: ResetPoint,
        reason: str,
        now: datetime,
    ) -> tuple[ExecutionRecord, ExecutionRecord]:
        return await self._call(
            "reset_run", lambda: self._inner.reset_run(run_id, new_run_id, to, reason, now)
        )

    # ------------------------------------------------------------------ ticks

    async def claim_tick(
        self,
        worker_id: str,
        now: datetime,
        lease: timedelta,
        workflow_names: Collection[str] | None = None,
        build_id: str | None = None,
    ) -> ExecutionRecord | None:
        lease = self._leased("claim_tick", lease)
        return await self._call(
            "claim_tick",
            lambda: self._inner.claim_tick(worker_id, now, lease, workflow_names, build_id),
        )

    async def complete_tick(
        self, run_id: str, worker_id: str, outcome: TickOutcome, now: datetime
    ) -> list[Event]:
        return await self._call(
            "complete_tick", lambda: self._inner.complete_tick(run_id, worker_id, outcome, now)
        )

    async def fail_tick(
        self,
        run_id: str,
        worker_id: str,
        error: str,
        retry_at: datetime,
        now: datetime,
    ) -> None:
        await self._call(
            "fail_tick", lambda: self._inner.fail_tick(run_id, worker_id, error, retry_at, now)
        )

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
        lease = self._leased("claim_step", lease)
        return await self._call(
            "claim_step",
            lambda: self._inner.claim_step(worker_id, now, lease, step_names, queues, build_id),
        )

    async def finish_step(
        self,
        job_id: str,
        worker_id: str,
        event: StepCompleted | StepFailed,
        now: datetime,
    ) -> bool:
        return await self._call(
            "finish_step", lambda: self._inner.finish_step(job_id, worker_id, event, now)
        )

    async def retry_step(
        self,
        job_id: str,
        worker_id: str,
        ready_at: datetime,
        failure: FailureInfo,
        now: datetime,
    ) -> bool:
        return await self._call(
            "retry_step",
            lambda: self._inner.retry_step(job_id, worker_id, ready_at, failure, now),
        )

    async def renew_step_lease(self, job_id: str, worker_id: str, expires_at: datetime) -> bool:
        return await self._call(
            "renew_step_lease",
            lambda: self._inner.renew_step_lease(job_id, worker_id, expires_at),
        )

    async def heartbeat_step(
        self,
        job_id: str,
        worker_id: str,
        expires_at: datetime,
        details: Payload | None,
        now: datetime,
    ) -> HeartbeatResult | None:
        return await self._call(
            "heartbeat_step",
            lambda: self._inner.heartbeat_step(job_id, worker_id, expires_at, details, now),
        )

    async def release_step(
        self, job_id: str, worker_id: str, now: datetime, *, consume_attempt: bool
    ) -> bool:
        return await self._call(
            "release_step",
            lambda: self._inner.release_step(
                job_id, worker_id, now, consume_attempt=consume_attempt
            ),
        )

    async def next_step_ready_at(
        self,
        step_names: Collection[str] | None = None,
        queues: Collection[str] | None = None,
    ) -> datetime | None:
        return await self._inner.next_step_ready_at(step_names, queues)

    async def get_step_jobs(self, run_id: str) -> list[StepJob]:
        return await self._inner.get_step_jobs(run_id)

    # ------------------------------------------------------------------ worker builds

    async def repin_run(self, run_id: str, build_id: str | None, now: datetime) -> ExecutionRecord:
        return await self._call("repin_run", lambda: self._inner.repin_run(run_id, build_id, now))

    async def declare_build_compatible(
        self, build_id: str, compatible_with: str, now: datetime
    ) -> None:
        await self._call(
            "declare_build_compatible",
            lambda: self._inner.declare_build_compatible(build_id, compatible_with, now),
        )

    async def revoke_build_compatibility(self, build_id: str, compatible_with: str) -> bool:
        return await self._call(
            "revoke_build_compatibility",
            lambda: self._inner.revoke_build_compatibility(build_id, compatible_with),
        )

    async def list_build_compatibility(self) -> list[BuildCompatibility]:
        return await self._inner.list_build_compatibility()

    async def heartbeat_worker(
        self, worker_id: str, build_id: str, now: datetime, expires_at: datetime
    ) -> None:
        await self._call(
            "heartbeat_worker",
            lambda: self._inner.heartbeat_worker(worker_id, build_id, now, expires_at),
        )

    async def list_worker_builds(self, now: datetime) -> list[WorkerPresence]:
        return await self._inner.list_worker_builds(now)

    async def count_pinned_runs(self) -> dict[str, int]:
        return await self._inner.count_pinned_runs()

    # ------------------------------------------------------------------ visibility

    async def query_executions(
        self, query: str, schema: SearchAttributes, now: datetime, limit: int = 100
    ) -> list[ExecutionRecord]:
        return await self._inner.query_executions(query, schema, now, limit)

    async def get_search_attributes(self, run_id: str) -> dict[str, object]:
        return await self._inner.get_search_attributes(run_id)

    # ------------------------------------------------------------------ outbox

    async def relay_outbox(self, now: datetime, limit: int = 100) -> int:
        return await self._call("relay_outbox", lambda: self._inner.relay_outbox(now, limit))

    async def outbox_size(self) -> int:
        return await self._inner.outbox_size()

    async def list_children(self, run_id: str) -> list[ExecutionRecord]:
        return await self._inner.list_children(run_id)

    # ------------------------------------------------------------------ schedules

    async def create_schedule(self, state: ScheduleState, now: datetime) -> ScheduleState:
        return await self._call("create_schedule", lambda: self._inner.create_schedule(state, now))

    async def get_schedule(self, schedule_id: str) -> ScheduleState:
        return await self._inner.get_schedule(schedule_id)

    async def list_schedules(self, limit: int = 100) -> list[ScheduleState]:
        return await self._inner.list_schedules(limit)

    async def update_schedule(
        self, state: ScheduleState, expected_version: int, now: datetime
    ) -> ScheduleState:
        return await self._call(
            "update_schedule",
            lambda: self._inner.update_schedule(state, expected_version, now),
        )

    async def delete_schedule(self, schedule_id: str) -> bool:
        return await self._call("delete_schedule", lambda: self._inner.delete_schedule(schedule_id))

    async def due_schedules(self, now: datetime, limit: int = 100) -> list[ScheduleState]:
        return await self._inner.due_schedules(now, limit)

    async def fire_schedule(
        self,
        state: ScheduleState,
        expected_version: int,
        starts: Sequence[ChildSpec],
        outbox: Sequence[OutboxMessage],
        now: datetime,
    ) -> ScheduleState:
        return await self._call(
            "fire_schedule",
            lambda: self._inner.fire_schedule(state, expected_version, starts, outbox, now),
        )

    async def next_schedule_at(self) -> datetime | None:
        return await self._inner.next_schedule_at()

    # ------------------------------------------------------------------ retention

    async def delete_closed_before(self, cutoff: datetime, limit: int = 1000) -> int:
        return await self._call(
            "delete_closed_before", lambda: self._inner.delete_closed_before(cutoff, limit)
        )

    # ------------------------------------------------------------------ timers

    async def next_timer_at(self) -> datetime | None:
        return await self._inner.next_timer_at()

    async def fire_due_timers(self, now: datetime, limit: int = 100) -> int:
        return await self._call("fire_due_timers", lambda: self._inner.fire_due_timers(now, limit))
