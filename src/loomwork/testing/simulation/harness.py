"""The simulation runner.

:func:`simulate` seeds every source of nondeterminism from one integer, builds a fresh
registry of workloads (:mod:`loomwork.testing.simulation.workloads`), a fault-injecting
store (:mod:`loomwork.testing.simulation.faults`) and a pool of chaotic workers
(:mod:`loomwork.testing.simulation.chaos`) on top of either a :class:`MemoryStore` or a
:class:`SQLiteStore`, drives every workload's runs through them, checks the invariants in
:mod:`loomwork.testing.simulation.invariants` as each run closes and again at the end,
and returns a :class:`SimulationReport`. Calling it again with the same seed and config
reproduces the same run -- see ``docs/simulation-testing.md`` for exactly what that
guarantee covers.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, TypeVar

from loomwork.client.client import Client
from loomwork.client.handle import WorkflowHandle
from loomwork.clock import Clock, ManualClock, SystemClock
from loomwork.codec.converter import default_converter
from loomwork.engine.config import EngineConfig
from loomwork.engine.engine import Engine
from loomwork.errors import (
    WorkflowAlreadyStartedError,
    WorkflowCanceledError,
    WorkflowFailedError,
    WorkflowTerminatedError,
    WorkflowTimedOutError,
)
from loomwork.retry import RetryPolicy
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.memory import MemoryStore
from loomwork.store.notify import TOPIC_OUTBOX
from loomwork.store.records import ExecutionStatus
from loomwork.testing.simulation.chaos import ChaosEvent, WorkerChaos
from loomwork.testing.simulation.faults import FaultConfig, FaultEvent, FaultyStore, InjectedFault
from loomwork.testing.simulation.invariants import InvariantChecker, Violation
from loomwork.testing.simulation.loop import run_simulated
from loomwork.testing.simulation.workloads import (
    RunPlan,
    SideEffectLog,
    Workload,
    build_workloads,
)
from loomwork.worker.worker import WorkerOptions

__all__ = ["RunRecord", "SimulationConfig", "SimulationReport", "simulate"]

T = TypeVar("T")

#: Tuned so that whatever real time a simulation does spend -- an actual notification
#: wake-up, a retry backoff too small for time-skipping to bother with -- stays
#: negligible. Workflow-visible timing against the memory backend is otherwise driven
#: entirely by its ManualClock; see loomwork.testing.TESTING_CONFIG for the same idea.
_MEMORY_CONFIG = EngineConfig(
    tick_lease=timedelta(seconds=2),
    tick_retry=RetryPolicy(initial_interval=0.001, max_interval=0.01, max_attempts=0, jitter=0),
    default_step_timeout=timedelta(seconds=2),
    default_step_retry=RetryPolicy(initial_interval=0.001, max_interval=0.01, jitter=0),
    step_lease_grace=timedelta(milliseconds=50),
    poll_interval=0.01,
)

#: For the SQLite backend there is no clock to skip (see _skip_idle_time's docstring), so
#: every durable wait costs real time; kept as small as the engine allows.
_SQLITE_CONFIG = EngineConfig(
    tick_lease=timedelta(seconds=2),
    tick_retry=RetryPolicy(initial_interval=0.005, max_interval=0.05, max_attempts=0, jitter=0),
    default_step_timeout=timedelta(seconds=2),
    default_step_retry=RetryPolicy(initial_interval=0.005, max_interval=0.05, jitter=0),
    step_lease_grace=timedelta(milliseconds=200),
    poll_interval=0.02,
)

_MAX_CLIENT_RETRIES = 200


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    """Everything about a simulation that is not the seed itself.

    Attributes:
        store: ``"memory"`` (fast, auto time-skipped, the default suite's backend) or
            ``"sqlite"`` (a real embedded database in a temporary file; slower, no time
            skipping, meant for the soak-testing CLI entry point).
        num_runs: workflow runs started, cycling through the selected workloads.
        num_workers: workers in the pool :class:`~...chaos.WorkerChaos` manages.
        fault_rate: overall store fault probability; see :meth:`fault_config`.
        lease_pressure: probability ``claim_tick``/``claim_step`` get a squeezed lease.
        kill_rate, cache_evict_rate: relative weights fed to the chaos scheduler.
        min_live_workers: the chaos scheduler never kills its way below this many.
        wall_clock_timeout: real seconds each run gets to close; a run that takes longer
            is a liveness violation, not just a slow test (see ``docs/simulation-testing.md``
            on why this is real time even against the time-skipped memory backend).
        workloads: restrict to a subset of workload names (``None`` means all).
    """

    store: Literal["memory", "sqlite"] = "memory"
    num_runs: int = 20
    num_workers: int = 3
    fault_rate: float = 0.1
    lease_pressure: float = 0.05
    kill_rate: float = 0.3
    cache_evict_rate: float = 0.3
    min_live_workers: int = 1
    wall_clock_timeout: float = 20.0
    workloads: frozenset[str] | None = None

    def fault_config(self) -> FaultConfig:
        return FaultConfig.uniform(self.fault_rate, lease_pressure=self.lease_pressure)


@dataclass(slots=True)
class RunRecord:
    """What happened to one scripted run, for the report."""

    run_id: str
    workload: str
    plan: RunPlan
    outcome: str | None = None
    detail: str = ""


@dataclass(slots=True)
class SimulationReport:
    """The result of one :func:`simulate` call."""

    seed: int
    config: SimulationConfig
    started: int = 0
    closed: int = 0
    violations: list[Violation] = field(default_factory=list)
    faults: list[FaultEvent] = field(default_factory=list)
    chaos: list[ChaosEvent] = field(default_factory=list)
    runs: list[RunRecord] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return not self.violations

    def format_text(self) -> str:
        lines = [
            f"seed={self.seed} store={self.config.store} runs={self.started} "
            f"closed={self.closed} faults={len(self.faults)} chaos={len(self.chaos)} "
            f"duration={self.duration_seconds:.2f}s",
        ]
        if self.ok:
            lines.append("OK: every invariant held.")
            return "\n".join(lines)
        lines.append(f"FAILED: {len(self.violations)} violation(s).")
        lines.append(f"Reproduce with: simulate({self.seed}, {self.config!r})")
        lines.append("")
        lines.append("violations:")
        lines.extend(f"  {v}" for v in self.violations)
        failing_runs = [r for r in self.runs if r.outcome not in (None, "ok")]
        if failing_runs:
            lines.append("")
            lines.append("runs that did not match their plan:")
            lines.extend(
                f"  {r.run_id} ({r.workload}): {r.outcome} -- {r.detail}" for r in failing_runs
            )
        if self.faults or self.chaos:
            lines.append("")
            lines.append("full injected-fault and chaos trace (bounded by construction):")
            lines.extend(f"  fault {f}" for f in self.faults)
            lines.extend(f"  chaos {c}" for c in self.chaos)
        return "\n".join(lines)


class _ReliableHandle:
    """Retries a client-facing call when the fault schedule makes it fail client-side,
    using a stable idempotency key so a retried send is never double-applied.

    A worker retries its own operations as a matter of course -- that is the entire
    point of the engine. A one-shot call made directly from application code (starting a
    run, sending a signal) is not automatically retried by anything, so the harness does
    here exactly what a production caller is expected to do, and no more.
    """

    def __init__(self, handle: WorkflowHandle[Any], workflow_id: str) -> None:
        self._handle = handle
        self._seq = 0
        self.workflow_id = workflow_id

    def _next_id(self) -> str:
        self._seq += 1
        return f"{self.workflow_id}-msg-{self._seq}"

    async def signal(self, channel: Any, value: Any) -> None:
        message_id = self._next_id()
        await _retrying(lambda: self._handle.signal(channel, value, message_id=message_id))

    async def update(self, name: str, *args: Any) -> Any:
        update_id = self._next_id()
        return await _retrying(lambda: self._handle.update(name, *args, update_id=update_id))

    async def cancel(self, reason: str = "") -> bool:
        return await _retrying(lambda: self._handle.cancel(reason))

    async def query(self, name: str, *args: Any) -> Any:
        return await _retrying(lambda: self._handle.query(name, *args))


async def _retrying(action: Callable[[], Awaitable[T]]) -> T:
    for _ in range(_MAX_CLIENT_RETRIES):
        try:
            return await action()
        except InjectedFault:
            await asyncio.sleep(0)
    raise RuntimeError(f"action did not succeed after {_MAX_CLIENT_RETRIES} injected faults")


async def _start(
    client: Client, workload: Workload, plan: RunPlan, workflow_id: str
) -> WorkflowHandle[Any]:
    for _ in range(_MAX_CLIENT_RETRIES):
        try:
            return await client.start(workload.entry, *plan.args, id=workflow_id)
        except InjectedFault:
            await asyncio.sleep(0)
        except WorkflowAlreadyStartedError:
            # The store committed the previous attempt; only its reply was lost. Resolve
            # a concrete run id (a bare-workflow-id handle would otherwise silently
            # follow whatever run is *latest* by the time each later call runs, which is
            # not what "the run this driver started" means once continue-as-new is in
            # play).
            handle = client.get_handle(workflow_id)
            record = await handle.describe()
            return client.get_handle(workflow_id, run_id=record.run_id)
    raise RuntimeError(f"could not start {workflow_id} after {_MAX_CLIENT_RETRIES} injected faults")


def _status_of(exc: Exception) -> str:
    if isinstance(exc, WorkflowFailedError):
        return "failed"
    if isinstance(exc, WorkflowCanceledError):
        return "canceled"
    if isinstance(exc, WorkflowTimedOutError):
        return "timed_out"
    if isinstance(exc, WorkflowTerminatedError):
        return "terminated"
    return f"unexpected:{type(exc).__name__}"


async def _drive_one(
    client: Client,
    workload: Workload,
    plan: RunPlan,
    tracked: dict[str, RunRecord],
    checker: InvariantChecker,
    store: Store,
    log: SideEffectLog,
    wall_clock_timeout: float,
) -> None:
    workflow_id = f"{plan.workload}:{plan.args[0]}"
    handle = await _start(client, workload, plan, workflow_id)
    # client.start always creates a concrete run and sets both ids on the handle; only
    # a handle built from a bare workflow id (never done here) would leave run_id unset.
    assert handle.run_id is not None
    record = RunRecord(run_id=handle.run_id, workload=plan.workload, plan=plan)
    tracked[record.run_id] = record
    try:
        # A workload never sees a raw store fault directly (only through _ReliableHandle
        # and _start's own retries), but anything else unexpected here is a genuine
        # finding, not a reason to abort every other run in flight: record it and move
        # on, exactly like every other mismatch this function can find.
        try:
            if plan.drive is not None:
                await plan.drive(_ReliableHandle(handle, workflow_id))
            result = await handle.result(timeout=wall_clock_timeout)
        except TimeoutError:
            record.outcome = "liveness_timeout"
            checker.violations.append(
                Violation(record.run_id, "liveness", f"did not close within {wall_clock_timeout}s")
            )
            return
        except (
            WorkflowFailedError,
            WorkflowCanceledError,
            WorkflowTerminatedError,
            WorkflowTimedOutError,
        ) as exc:
            _check_closed_outcome(record, exc, checker)
            return
        except Exception as exc:  # surfaced as a violation, never a crash of the whole simulation
            record.outcome = "unexpected_error"
            record.detail = repr(exc)
            checker.violations.append(Violation(record.run_id, "unexpected_error", repr(exc)))
            return
        _check_completed_outcome(record, result, checker)
        if record.outcome == "ok" and plan.post_check is not None:
            try:
                await plan.post_check(_ReliableHandle(handle, workflow_id))
            except AssertionError as exc:
                record.outcome = "post_check_failed"
                record.detail = str(exc)
                checker.violations.append(Violation(record.run_id, "post_check", str(exc)))
    finally:
        await _check_history(record, checker, store)
        counts = {key: log.count(key) for key in plan.step_keys}
        checker.check_execution_counts(record.run_id, counts)
        execution = await store.get_execution(record.run_id)
        await checker.check_no_orphaned_work(store, execution)


def _check_closed_outcome(record: RunRecord, exc: Exception, checker: InvariantChecker) -> None:
    status = _status_of(exc)
    expected = record.plan.expected
    if status != expected.status:
        record.outcome = "status_mismatch"
        record.detail = f"expected {expected.status}, got {status} ({exc})"
        checker.violations.append(Violation(record.run_id, "expected_outcome", record.detail))
    elif expected.failure_contains and expected.failure_contains not in str(exc):
        record.outcome = "failure_mismatch"
        record.detail = f"expected failure to mention {expected.failure_contains!r}: {exc}"
        checker.violations.append(Violation(record.run_id, "expected_outcome", record.detail))
    else:
        record.outcome = "ok"


def _check_completed_outcome(record: RunRecord, result: Any, checker: InvariantChecker) -> None:
    expected = record.plan.expected
    if expected.status != "completed":
        record.outcome = "status_mismatch"
        record.detail = f"expected {expected.status}, run completed with {result!r}"
        checker.violations.append(Violation(record.run_id, "expected_outcome", record.detail))
    elif result != expected.result:
        record.outcome = "result_mismatch"
        record.detail = f"expected {expected.result!r}, got {result!r}"
        checker.violations.append(Violation(record.run_id, "expected_outcome", record.detail))
    else:
        record.outcome = "ok"


async def _check_history(record: RunRecord, checker: InvariantChecker, store: Store) -> None:
    history = await store.load_history(record.run_id)
    checker.check_history_shape(record.run_id, history)
    checker.check_exactly_once_recording(record.run_id, history)
    checker.check_replays(record.run_id, history, status=record.outcome or "unknown")


@dataclass(slots=True)
class _Backend:
    """The store/clock/config triple :func:`_run` builds up front, and how to tear it
    down again -- pulled out of :func:`_run` only to keep that function short."""

    clock: Clock
    raw_store: Store
    engine_config: EngineConfig
    tmp_dir: TemporaryDirectory[str] | None = None

    async def close(self, store_kind: Literal["memory", "sqlite"]) -> None:
        if store_kind == "sqlite":
            await self.raw_store.close()
        if self.tmp_dir is not None:
            self.tmp_dir.cleanup()


async def _build_backend(config: SimulationConfig) -> _Backend:
    if config.store == "memory":
        return _Backend(ManualClock(), MemoryStore(), _MEMORY_CONFIG)
    from loomwork.store.sqlite import SQLiteStore

    tmp_dir = TemporaryDirectory()
    raw_store = SQLiteStore(Path(tmp_dir.name) / "simulation.db")
    await raw_store.open()
    return _Backend(SystemClock(), raw_store, _SQLITE_CONFIG, tmp_dir)


async def _run(seed: int, config: SimulationConfig) -> SimulationReport:
    master = random.Random(seed)
    fault_seed = master.randrange(2**31)
    chaos_seed = master.randrange(2**31)
    workload_seed = master.randrange(2**31)

    log = SideEffectLog()
    workloads = build_workloads(log, names=config.workloads)
    if not workloads:
        raise ValueError("no workloads selected")
    registry = Registry(
        [w for wl in workloads for w in wl.workflows],
        [s for wl in workloads for s in wl.steps],
    )
    converter = default_converter
    backend = await _build_backend(config)
    store = FaultyStore(backend.raw_store, seed=fault_seed, config=config.fault_config())
    engine = Engine(store, clock=backend.clock, config=backend.engine_config)
    client = Client(engine, converter, registry=registry)
    worker_options = WorkerOptions(workflow_concurrency=4, step_concurrency=8, shutdown_grace=0)

    tracked: dict[str, RunRecord] = {}
    checker = InvariantChecker(registry, converter)
    pool = WorkerChaos(
        store,
        registry,
        backend.clock,
        backend.engine_config,
        seed=chaos_seed,
        worker_options=worker_options,
        num_workers=config.num_workers,
        kill_rate=config.kill_rate,
        cache_evict_rate=config.cache_evict_rate,
        run_ids=lambda: list(tracked),
        min_live_workers=config.min_live_workers,
    )

    workload_rng = random.Random(workload_seed)
    plans = [
        (workloads[i % len(workloads)], workloads[i % len(workloads)].plan(workload_rng, i))
        for i in range(config.num_runs)
    ]

    idle_task: asyncio.Task[None] | None = None
    try:
        await pool.start()
        if isinstance(backend.raw_store, MemoryStore) and isinstance(backend.clock, ManualClock):
            idle_task = asyncio.ensure_future(
                _skip_idle_time(backend.raw_store, pool, backend.clock)
            )
        await asyncio.gather(
            *(
                _drive_one(
                    client, workload, plan, tracked, checker, store, log, config.wall_clock_timeout
                )
                for workload, plan in plans
            )
        )
        # Every tracked run has closed, but a message its last tick enqueued (a result
        # to a parent, a parent-close policy to a child) may not have reached the relay
        # loop's next poll yet: give it a moment before checking that the outbox drained,
        # instead of asserting a timing artifact of stopping the pool immediately.
        await _wait_outbox_drained(store)
        # Still with the store open: closing it (in `finally`, for the SQLite backend)
        # would make every one of these checks' queries fail outright, not just find
        # nothing.
        await _finalize_store_checks(checker, store, tracked)
    finally:
        if idle_task is not None:
            idle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await idle_task
        await pool.stop()
        await backend.close(config.store)
    return _report(seed, config, tracked, checker, store, pool)


async def _wait_outbox_drained(store: Store, timeout: float = 2.0) -> None:
    """Best-effort wait for the outbox relay to catch up, real time bounded.

    The relay loop runs on its own real-time poll cadence, independent of the
    (possibly time-skipped) workflow clock; a message enqueued by the very last tick of
    the very last run to close may need one more of its cycles. Never raises: a message
    still queued after ``timeout`` is a genuine ``outbox_drained`` violation for
    :meth:`InvariantChecker.check_outbox_drained` to report, not something to hide here.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while await store.outbox_size():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return
        token = store.notifier.token(TOPIC_OUTBOX)
        await store.notifier.wait(TOPIC_OUTBOX, token, min(remaining, 0.05))


async def _finalize_store_checks(
    checker: InvariantChecker, store: Store, tracked: dict[str, RunRecord]
) -> None:
    """Checks that only make sense once every driven run has closed, plus a sweep over
    every *other* execution the store holds -- children and continue-as-new successors,
    which a workload never hands the harness a handle for directly, but which must obey
    exactly the same invariants as a directly-driven run."""
    run_ids = list(tracked)
    await checker.check_no_leftover_timers(store, run_ids)
    await checker.check_outbox_drained(store, run_ids)
    for record in await store.list_executions(limit=100_000):
        if record.run_id in tracked:
            continue
        history = await store.load_history(record.run_id)
        checker.check_history_shape(record.run_id, history)
        checker.check_exactly_once_recording(record.run_id, history)
        if record.is_closed:
            checker.check_replays(record.run_id, history, status=record.status.value)
            await checker.check_no_orphaned_work(store, record)


def _report(
    seed: int,
    config: SimulationConfig,
    tracked: dict[str, RunRecord],
    checker: InvariantChecker,
    store: FaultyStore,
    pool: WorkerChaos,
) -> SimulationReport:
    return SimulationReport(
        seed=seed,
        config=config,
        started=len(tracked),
        closed=sum(1 for r in tracked.values() if r.outcome is not None),
        violations=checker.violations,
        faults=list(store.injected),
        chaos=list(pool.events),
        runs=list(tracked.values()),
    )


@dataclass(frozen=True, slots=True)
class _Snapshot:
    """A point-in-time reading of everything idle-detection needs to decide, across
    every worker in the pool -- the same idea as ``WorkflowEnvironment``'s ``_Snapshot``
    (``src/loomwork/testing/environment.py``), generalised from one worker to several."""

    busy: int
    tick_at: datetime | None
    step_at: datetime | None
    timer_at: datetime | None
    schedule_at: datetime | None
    outbox: int

    def actionable(self, now: datetime) -> bool:
        if self.busy or self.outbox:
            return True
        return any(
            deadline is not None and deadline <= now
            for deadline in (self.tick_at, self.step_at, self.timer_at, self.schedule_at)
        )

    def next_deadline(self, now: datetime) -> datetime | None:
        future = [
            deadline
            for deadline in (self.tick_at, self.step_at, self.timer_at, self.schedule_at)
            if deadline is not None and deadline > now
        ]
        return min(future, default=None)


async def _snapshot(store: MemoryStore, pool: WorkerChaos) -> _Snapshot:
    tick_at, step_at, timer_at, schedule_at, outbox = await asyncio.gather(
        store.next_tick_ready_at(),
        store.next_step_ready_at(),
        store.next_timer_at(),
        store.next_schedule_at(),
        store.outbox_size(),
    )
    busy = sum(w.busy for w in pool.workers)
    return _Snapshot(busy, tick_at, step_at, timer_at, schedule_at, outbox)


async def _leased_deadlines(store: MemoryStore) -> list[datetime]:
    """Lease expiries of currently-claimed ticks and step jobs, across every running
    execution -- the one class of future deadline ``next_tick_ready_at``/
    ``next_step_ready_at`` cannot see, because both only ever report *unleased* work
    (see their docstrings). Production time-skipping never needs this: a lease is only
    ever abandoned by a worker dying, which idle-detection notices as "busy" dropping to
    zero, with the abandoned lease's own expiry already visible as ordinary ready-at
    metadata once it lapses. Under fault injection a lease can also be abandoned by a
    call whose *reply* was lost after it committed (see ``faults.py``): the caller thinks
    the operation never happened and moves on, but the store already recorded the claim,
    which is invisible to every ``next_*_ready_at`` query until this function's deadline
    is reached and the normal poll notices the lease is free again.
    """
    deadlines: list[datetime] = []
    for record in await store.list_executions(status=ExecutionStatus.RUNNING, limit=100_000):
        if record.lease_owner is not None and record.lease_expires_at is not None:
            deadlines.append(record.lease_expires_at)
        jobs = await store.get_step_jobs(record.run_id)
        deadlines.extend(
            job.lease_expires_at
            for job in jobs
            if job.lease_owner is not None and job.lease_expires_at is not None
        )
    return deadlines


async def _skip_idle_time(store: MemoryStore, pool: WorkerChaos, clock: ManualClock) -> None:
    """Jump ``clock`` straight to the next deadline once nothing can make progress.

    Idle means no worker in the pool is mid-tick or mid-step, no tick, step, timer or
    schedule is already due, and the outbox is empty. Reads ``store`` (the unwrapped
    backend, not the ``FaultyStore`` in front of it) directly so that idle-detection
    itself is never delayed or lied to by the fault schedule -- only the engine's own use
    of the store is faulted, exactly like the injected latency in
    ``tests/integration/test_chaos.py`` never touches the test's own assertions.
    """
    stability_yields = 5
    busy_poll_seconds = 0.001
    idle_fallback_seconds = 0.02
    while True:
        snapshot = await _snapshot(store, pool)
        if snapshot.actionable(clock.now()):
            await asyncio.sleep(busy_poll_seconds if snapshot.busy else 0)
            continue
        settled = True
        for _ in range(stability_yields):
            await asyncio.sleep(0)
            if sum(w.busy for w in pool.workers):
                settled = False
                break
        if not settled:
            continue
        snapshot = await _snapshot(store, pool)
        now = clock.now()
        if snapshot.actionable(now):
            continue
        target = snapshot.next_deadline(now)
        candidates = [d for d in await _leased_deadlines(store) if d > now]
        if target is not None:
            candidates.append(target)
        target = min(candidates, default=None)
        if target is None:
            await asyncio.sleep(idle_fallback_seconds)
            continue
        await clock.set(target)


def simulate(seed: int, config: SimulationConfig | None = None) -> SimulationReport:
    """Run one simulation seed to completion and report what it found.

    Every source of nondeterminism -- the event loop's callback order (:mod:`...loop`),
    fault injection (:mod:`...faults`), worker chaos (:mod:`...chaos`), and workload
    argument/script generation (:mod:`...workloads`) -- is seeded from ``seed`` alone,
    so calling this again with the same ``seed`` and ``config`` reproduces the exact same
    run (see ``docs/simulation-testing.md`` for the precise boundary of that guarantee).
    """
    resolved = config or SimulationConfig()
    started = time.monotonic()
    report = run_simulated(seed, lambda: _run(seed, resolved))
    report.duration_seconds = time.monotonic() - started
    return report
