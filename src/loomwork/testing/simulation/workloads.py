"""A library of workflows the simulation drives.

Each :class:`Workload` bundles workflow and step definitions with a ``plan`` function
that, given a seeded :class:`random.Random` and a run index, produces a
:class:`RunPlan`: arguments, an :class:`ExpectedOutcome` computed *without running any
workflow code*, and (for workloads that race a signal against a timer, or cancel a run)
a ``drive`` coroutine describing exactly how the harness interacts with the run. Because
the expected outcome is a pure function of the plan -- never of what the engine actually
did -- the harness can tell a correct-but-unlucky interleaving apart from a real bug.

Every workload is built fresh per simulation run by :func:`build_workloads`, closing
over a private :class:`SideEffectLog`: this keeps two concurrent (or sequential, reusing
the same process) simulations from sharing step-execution counters, and lets the
invariant checker demand "this step call site executed at least once" without reading
anything from the store, which only ever records the *outcome* the engine decided to
keep (exactly once), not how many attempts it took to get there.

Together the workloads exercise: retries (``retry_parallel``), parallel steps
(``retry_parallel``), durable timers and timeouts (``retry_parallel``, ``signal_race``),
signals and updates (``signal_race``), child workflows and sagas (``saga_child``),
class-based workflows and continue-as-new (``counter``), and cancellation
(``cancellation``).
"""

from __future__ import annotations

import asyncio
import random
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

import loomwork as lw
from loomwork.sdk.definitions import WorkflowLike

__all__ = [
    "DriveHandle",
    "ExpectedOutcome",
    "RunPlan",
    "SideEffectLog",
    "Workload",
    "build_workloads",
]

#: Every step whose own outcome is not the thing under test gets an unlimited retry
#: budget: the simulation's fault schedule can force a step to be attempted (and
#: abandoned -- its reply lost to a fault, its worker killed) many more times than a
#: production default (5) would allow, and that must never turn into a *business*
#: failure a workload's expected outcome cannot predict. A bounded retry budget stays
#: meaningful only for the one workload that deliberately tests it (``retry_parallel``,
#: whose failure count is itself part of the plan) and for a deliberately
#: non-retryable failure (``saga_child``'s "declined" charge, see below).
_UNLIMITED_RETRY = lw.RetryPolicy(
    initial_interval=0.001, max_interval=0.01, max_attempts=0, jitter=0
)


class SideEffectLog:
    """Counts step executions by an arbitrary key, scoped to one simulation run.

    The store and recorded history only ever show the *decision* an engine made
    (exactly one recorded outcome per step, per the "decisions are recorded exactly
    once" invariant); they cannot show how many times a step's body actually ran before
    that decision was recorded. Steps in this module call :meth:`record` on every
    attempt, independent of retries or duplicate execution, so the checker can verify
    "at least once" against something the store itself cannot answer.
    """

    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()

    def record(self, key: str) -> int:
        self._counts[key] += 1
        return self._counts[key]

    def count(self, key: str) -> int:
        return self._counts[key]


class DriveHandle(Protocol):
    """What a ``drive``/``post_check`` callable can do to a running workflow.

    A structural subset of :class:`~loomwork.client.handle.WorkflowHandle`: the harness
    hands workloads a wrapper that retries an operation transparently when the fault
    schedule makes it fail client-side (see ``docs/simulation-testing.md``), using a
    stable idempotency key so a retried send is never double-applied. Workloads never
    see a raw handle, and never need to think about the fault schedule at all.
    """

    async def signal(self, channel: Any, value: Any) -> None: ...
    async def update(self, name: str, *args: Any) -> Any: ...
    async def cancel(self, reason: str = "") -> bool: ...
    async def query(self, name: str, *args: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class ExpectedOutcome:
    """What a run's :meth:`~loomwork.client.handle.WorkflowHandle.result` should do.

    ``status`` is one of ``"completed"``, ``"failed"`` or ``"canceled"`` (the workload
    library never times a run out on its own execution timeout; ``"timed_out"`` would
    mean the same thing here but no workload needs it). ``failure_contains`` is checked
    against ``str(exc)`` of the exception :meth:`result` raises, when ``status`` is not
    ``"completed"``.
    """

    status: str
    result: Any = None
    failure_contains: str | None = None


@dataclass(frozen=True, slots=True)
class RunPlan:
    """One scripted run: its arguments, expected outcome, and how to drive it.

    ``drive`` is awaited with the freshly started handle immediately after
    ``start_workflow`` returns and before anything else touches that run -- the same
    guidance ``docs/testing.md`` gives for interacting with a run that auto time-skipping
    is free to jump straight past. ``post_check`` runs after the run has closed and the
    expected outcome has already been confirmed; it raises :class:`AssertionError` (via
    a plain ``assert``) for anything extra a workload wants verified.
    """

    workload: str
    args: tuple[Any, ...]
    expected: ExpectedOutcome
    step_keys: tuple[str, ...] = ()
    drive: Callable[[DriveHandle], Awaitable[None]] | None = None
    post_check: Callable[[DriveHandle], Awaitable[None]] | None = None


@dataclass(frozen=True, slots=True)
class Workload:
    """A registered workflow (plus any children it starts) and how to generate runs."""

    name: str
    entry: WorkflowLike
    workflows: tuple[WorkflowLike, ...]
    steps: tuple[lw.StepDefinition[Any, Any], ...]
    plan: Callable[[random.Random, int], RunPlan]


# ----------------------------------------------------------- A: retries, parallel steps, timer


def _build_retry_parallel(log: SideEffectLog) -> Workload:
    @lw.step(name="sim.retry_parallel.flaky", retry=_UNLIMITED_RETRY)
    async def flaky(key: str, fail_times: int) -> str:
        attempt = log.record(key)
        if attempt <= fail_times:
            raise ConnectionError(f"{key} attempt {attempt} injected failure")
        return key.upper()

    @lw.workflow(name="sim.retry_parallel")
    async def retry_parallel(run_key: str, fail_a: int, fail_b: int, delay_ms: int) -> list[str]:
        a, b = await asyncio.gather(flaky(f"{run_key}.a", fail_a), flaky(f"{run_key}.b", fail_b))
        await lw.sleep(timedelta(milliseconds=delay_ms))
        return sorted([a, b])

    def plan(rng: random.Random, index: int) -> RunPlan:
        run_key = f"retrypar-{index}"
        fail_a, fail_b = rng.randint(0, 3), rng.randint(0, 3)
        delay_ms = rng.randint(1, 20)
        expected = sorted([f"{run_key}.a".upper(), f"{run_key}.b".upper()])
        return RunPlan(
            workload="retry_parallel",
            args=(run_key, fail_a, fail_b, delay_ms),
            expected=ExpectedOutcome(status="completed", result=expected),
            step_keys=(f"{run_key}.a", f"{run_key}.b"),
        )

    return Workload(
        name="retry_parallel",
        entry=retry_parallel,
        workflows=(retry_parallel,),
        steps=(flaky,),
        plan=plan,
    )


# --------------------------------------------------------------------- B: signals, timeout, updates


_decision = lw.Channel("sim.signal_race.decision", str)


def _build_signal_race(log: SideEffectLog) -> Workload:
    @lw.step(name="sim.signal_race.record", retry=_UNLIMITED_RETRY)
    async def record_outcome(key: str, outcome: str) -> str:
        log.record(key)
        return outcome

    @lw.workflow(name="sim.signal_race")
    async def signal_race(run_key: str, timeout_ms: int) -> str:
        pings = 0

        @lw.update("ping")
        def ping() -> int:
            nonlocal pings
            pings += 1
            return pings

        @lw.query("pings")
        def get_pings() -> int:
            return pings

        try:
            async with asyncio.timeout(timeout_ms / 1000):
                reason = await _decision.receive()
            outcome = f"signaled:{reason}"
        except TimeoutError:
            outcome = "timed_out"
        return await record_outcome(run_key, outcome)

    def plan(rng: random.Random, index: int) -> RunPlan:
        run_key = f"race-{index}"
        # Durable timeouts cost no real time under time-skipping, so this can be as
        # generous as it likes; it only needs to comfortably outlast however long the
        # "signaled" branch's own update-then-signal round trip takes in real time under
        # the fault schedule (retried client-side sends, injected latency, ...) so that
        # branch is not itself racing the clock of a *different* run sharing it.
        timeout_ms = rng.randint(200, 1000)
        signaled = rng.random() < 0.5

        async def drive(handle: DriveHandle) -> None:
            # Whichever of these two calls could still race the durable timeout goes
            # first, with nothing else awaited in between: interacting with a run across
            # more than one await risks auto time-skipping jumping the shared clock past
            # the deadline in the gap between them (see docs/testing.md). The ping
            # update's own timing never affects the expected outcome, so it can safely
            # go second.
            if signaled:
                await handle.signal(_decision, "go")
            await handle.update("ping")

        async def post_check(handle: DriveHandle) -> None:
            pings = await handle.query("pings")
            assert pings == 1, f"expected exactly one update handled, got {pings}"

        expected_result = "signaled:go" if signaled else "timed_out"
        return RunPlan(
            workload="signal_race",
            args=(run_key, timeout_ms),
            expected=ExpectedOutcome(status="completed", result=expected_result),
            step_keys=(run_key,),
            drive=drive,
            post_check=post_check,
        )

    return Workload(
        name="signal_race",
        entry=signal_race,
        workflows=(signal_race,),
        steps=(record_outcome,),
        plan=plan,
    )


# --------------------------------------------------------------------- C: child workflow + saga


def _build_saga_child(log: SideEffectLog) -> Workload:
    @lw.step(name="sim.saga.child_step", retry=_UNLIMITED_RETRY)
    async def child_step(key: str) -> str:
        log.record(f"{key}.child_step")
        return f"{key}-child"

    @lw.workflow(name="sim.saga.helper")
    async def helper(run_key: str) -> str:
        return await child_step(run_key)

    @lw.step(name="sim.saga.reserve", retry=_UNLIMITED_RETRY)
    async def reserve(key: str) -> str:
        log.record(f"{key}.reserve")
        return f"{key}-reserved"

    @lw.step(name="sim.saga.release", retry=_UNLIMITED_RETRY)
    async def release(key: str) -> None:
        log.record(f"{key}.release")

    @lw.step(
        name="sim.saga.charge",
        retry=lw.RetryPolicy(
            initial_interval=0.001,
            max_interval=0.01,
            max_attempts=0,
            jitter=0,
            non_retryable_types=["Declined"],
        ),
    )
    async def charge(key: str, poison: bool) -> str:
        log.record(f"{key}.charge")
        if poison:
            raise lw.ApplicationError(f"{key} was declined", type="Declined")
        return f"{key}-charged"

    @lw.step(name="sim.saga.combine", retry=_UNLIMITED_RETRY)
    async def combine(key: str, child_result: str, reservation: str, charge_id: str) -> str:
        log.record(f"{key}.combine")
        return f"{key}:{child_result}:{reservation}:{charge_id}"

    @lw.workflow(name="sim.saga")
    async def saga_child(run_key: str, poison: bool) -> str:
        child = lw.start_child(helper, run_key)
        undo = lw.Compensations()
        try:
            reservation = await reserve(run_key)
            undo.add(release, run_key)
            charge_id = await charge(run_key, poison)
            undo.add(refund_noop, run_key)
            child_result = await child
            return await combine(run_key, child_result, reservation, charge_id)
        except Exception:
            await undo.run()
            raise

    @lw.step(name="sim.saga.refund", retry=_UNLIMITED_RETRY)
    async def refund_noop(key: str) -> None:
        log.record(f"{key}.refund")

    def plan(rng: random.Random, index: int) -> RunPlan:
        run_key = f"saga-{index}"
        poison = rng.random() < 0.5
        step_keys: tuple[str, ...]
        if poison:
            expected = ExpectedOutcome(status="failed", failure_contains="declined")
            step_keys = (f"{run_key}.reserve", f"{run_key}.charge", f"{run_key}.release")
        else:
            result = f"{run_key}:{run_key}-child:{run_key}-reserved:{run_key}-charged"
            expected = ExpectedOutcome(status="completed", result=result)
            step_keys = (
                f"{run_key}.reserve",
                f"{run_key}.charge",
                f"{run_key}.child_step",
                f"{run_key}.combine",
            )
        return RunPlan(
            workload="saga_child",
            args=(run_key, poison),
            expected=expected,
            step_keys=step_keys,
        )

    return Workload(
        name="saga_child",
        entry=saga_child,
        workflows=(saga_child, helper),
        steps=(child_step, reserve, release, charge, combine, refund_noop),
        plan=plan,
    )


# ----------------------------------------------------------- D: class-based, continue-as-new


def _build_counter(log: SideEffectLog) -> Workload:
    @lw.step(name="sim.counter.persist", retry=_UNLIMITED_RETRY)
    async def persist_total(key: str, value: int) -> int:
        log.record(key)
        return value

    @lw.workflow(name="sim.counter")
    class Counter:
        def __init__(
            self, run_key: str, start: int, expected_deltas: int, generation: int = 0
        ) -> None:
            self.run_key = run_key
            self.value = start
            self.received = 0
            self.expected_deltas = expected_deltas
            self.generation = generation

        @lw.run
        async def run(
            self, run_key: str, _start: int, _expected_deltas: int, generation: int = 0
        ) -> int:
            # __init__ is called with the same positional arguments just before this
            # runs and already captured them onto self; this signature only needs to
            # match it, hence the otherwise-unused leading-underscore parameter names.
            if self.received < self.expected_deltas:
                # No timeout: the driver sends every delta with its own retries, so the
                # condition is always eventually satisfied, and a durable timer here
                # would race auto time-skipping jumping the (shared) clock past it in
                # the gap between two of the driver's separately-awaited signal sends
                # (each is its own round trip; see the "signaled" branch of
                # sim.signal_race for the same hazard with a timeout that does matter).
                await lw.wait_condition(lambda: self.received >= self.expected_deltas)
            total = await persist_total(f"{run_key}#{generation}", self.value)
            if generation == 0:
                lw.continue_as_new(run_key, total, 0, 1, workflow=Counter)
            return total

        @lw.signal("delta")
        def add(self, amount: int) -> None:
            self.value += amount
            self.received += 1

        @lw.query("value")
        def current(self) -> int:
            return self.value

    def plan(rng: random.Random, index: int) -> RunPlan:
        run_key = f"counter-{index}"
        start = rng.randint(-5, 5)
        deltas = [rng.randint(-3, 3) for _ in range(rng.randint(0, 5))]

        async def drive(handle: DriveHandle) -> None:
            for amount in deltas:
                await handle.signal(Counter.add, amount)

        total = start + sum(deltas)
        return RunPlan(
            workload="counter",
            args=(run_key, start, len(deltas), 0),
            expected=ExpectedOutcome(status="completed", result=total),
            step_keys=(f"{run_key}#0", f"{run_key}#1"),
            drive=drive if deltas else None,
        )

    return Workload(
        name="counter",
        entry=Counter,
        workflows=(Counter,),
        steps=(persist_total,),
        plan=plan,
    )


# --------------------------------------------------------------------- E: cancellation


def _build_cancellation(log: SideEffectLog) -> Workload:
    @lw.step(name="sim.cancellation.work", retry=_UNLIMITED_RETRY)
    async def do_work(key: str) -> str:
        log.record(key)
        return f"{key}-done"

    @lw.workflow(name="sim.cancellation")
    async def cancellable(run_key: str, wait_ms: int, catch: bool) -> str:
        if catch:
            try:
                result = await do_work(run_key)
                await lw.sleep(timedelta(milliseconds=wait_ms))
                return f"{result}-slept"
            except asyncio.CancelledError:
                return f"{run_key}-caught"
        result = await do_work(run_key)
        await lw.sleep(timedelta(milliseconds=wait_ms))
        return f"{result}-slept"

    def plan(rng: random.Random, index: int) -> RunPlan:
        run_key = f"cancel-{index}"
        wait_ms = rng.randint(1, 20)
        cancel = rng.random() < 0.66
        catch = cancel and rng.random() < 0.5

        async def drive(handle: DriveHandle) -> None:
            if not cancel:
                return
            if catch:
                # Cancelling before the workflow's task has ever been stepped races its
                # own start: the engine delivers it before any of the function body
                # (including the try) has run, so no try/except inside it could ever
                # observe it -- not a scenario a workload can predict the outcome of.
                # Waiting for do_work's recorded side effect guarantees the workflow
                # reached (and is suspended inside) the try before cancellation lands.
                for _ in range(2000):
                    if log.count(run_key) >= 1:
                        break
                    await asyncio.sleep(0.001)
            await handle.cancel("simulated")

        if not cancel:
            expected = ExpectedOutcome(status="completed", result=f"{run_key}-done-slept")
        elif catch:
            expected = ExpectedOutcome(status="completed", result=f"{run_key}-caught")
        else:
            expected = ExpectedOutcome(status="canceled")
        # An immediate cancel can beat do_work's step job to its first claim -- closing
        # the run deletes the still-unclaimed job, and the step never runs at all. Only
        # require it once cancellation is not in the picture.
        return RunPlan(
            workload="cancellation",
            args=(run_key, wait_ms, catch),
            expected=expected,
            step_keys=() if cancel else (run_key,),
            drive=drive,
        )

    return Workload(
        name="cancellation",
        entry=cancellable,
        workflows=(cancellable,),
        steps=(do_work,),
        plan=plan,
    )


_BUILDERS: tuple[Callable[[SideEffectLog], Workload], ...] = (
    _build_retry_parallel,
    _build_signal_race,
    _build_saga_child,
    _build_counter,
    _build_cancellation,
)


def build_workloads(log: SideEffectLog, *, names: frozenset[str] | None = None) -> list[Workload]:
    """Fresh workflow/step definitions for one simulation run, closing over ``log``.

    Rebuilding definitions per call (rather than sharing module-level singletons) keeps
    simulation runs fully isolated: each gets its own side-effect counters and its own
    function/class objects to register with a fresh :class:`~loomwork.Registry`.
    """
    workloads = [builder(log) for builder in _BUILDERS]
    if names is not None:
        workloads = [w for w in workloads if w.name in names]
    return workloads
