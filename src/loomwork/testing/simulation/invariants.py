"""Checking the engine's invariants (``docs/internals.md``) against what a simulation
run actually recorded.

Every check here reads only through the public :class:`~loomwork.store.base.Store`
contract and :mod:`loomwork.versioning`, exactly what a real operator or CI job could
run against a production store -- nothing here reaches into store internals.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loomwork.codec.converter import DataConverter
from loomwork.history.events import Event, StepCompleted, StepFailed, TickCompleted
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.records import ExecutionRecord
from loomwork.versioning import check_history

__all__ = ["InvariantChecker", "Violation"]


@dataclass(frozen=True, slots=True)
class Violation:
    """One invariant that did not hold."""

    run_id: str
    check: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.check}] run {self.run_id}: {self.detail}"


@dataclass(slots=True)
class InvariantChecker:
    """Accumulates :class:`Violation` objects found while checking recorded state.

    Never raises: a simulation with a real bug should finish and report everything it
    found, not stop at the first assertion. Callers inspect :attr:`violations` (or the
    :attr:`ok` shortcut) once the simulation is done -- or after every run closes, for
    checks that only need that one run's history.
    """

    registry: Registry
    converter: DataConverter
    violations: list[Violation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def _fail(self, run_id: str, check: str, detail: str) -> None:
        self.violations.append(Violation(run_id, check, detail))

    # ------------------------------------------------------------------ per-history checks

    def check_history_shape(self, run_id: str, history: list[Event]) -> None:
        """Histories are append-only with dense, increasing event ids, and a
        ``TickCompleted.consumed_through`` never decreases within a run."""
        previous_id = 0
        previous_consumed = -1
        for event in history:
            if event.event_id != previous_id + 1:
                self._fail(
                    run_id,
                    "dense_history",
                    f"event id {event.event_id} follows {previous_id} "
                    f"(expected {previous_id + 1})",
                )
            previous_id = max(previous_id, event.event_id)
            if isinstance(event, TickCompleted):
                if event.consumed_through < previous_consumed:
                    self._fail(
                        run_id,
                        "monotonic_consumed_through",
                        f"consumed_through went from {previous_consumed} "
                        f"to {event.consumed_through}",
                    )
                previous_consumed = max(previous_consumed, event.consumed_through)

    def check_exactly_once_recording(self, run_id: str, history: list[Event]) -> None:
        """A step's outcome (``StepCompleted``/``StepFailed``) is recorded exactly once
        per sequence number, however many attempts it took (at-least-once execution,
        exactly-once recording; see :mod:`loomwork.testing.simulation.workloads` for the
        execution-count half of this check)."""
        seen: dict[int, int] = {}
        for event in history:
            if isinstance(event, StepCompleted | StepFailed):
                seen[event.seq] = seen.get(event.seq, 0) + 1
        for seq, count in seen.items():
            if count != 1:
                self._fail(
                    run_id,
                    "exactly_once_recording",
                    f"step seq={seq} has {count} recorded outcomes, expected 1",
                )

    def check_replays(self, run_id: str, history: list[Event], *, status: str) -> None:
        """The recorded history still replays cleanly against this registry's code."""
        result = check_history(history, self.registry, converter=self.converter, status=status)
        if not result.ok:
            self._fail(run_id, "replays_cleanly", result.error or "replay failed")

    def check_execution_counts(self, run_id: str, counts: dict[str, int]) -> None:
        """At-least-once execution: every step call site the workload expected to run
        recorded at least one attempt in the :class:`~...workloads.SideEffectLog`."""
        for key, count in counts.items():
            if count < 1:
                self._fail(run_id, "at_least_once_execution", f"{key!r} never executed")

    # ------------------------------------------------------------------ store-wide checks

    async def check_no_orphaned_work(self, store: Store, record: ExecutionRecord) -> None:
        """No step job survives a closed run."""
        if not record.is_closed:
            return
        jobs = await store.get_step_jobs(record.run_id)
        if jobs:
            self._fail(
                record.run_id,
                "no_orphaned_step_jobs",
                f"{len(jobs)} step job(s) still outstanding on a closed run",
            )

    async def check_no_leftover_timers(self, store: Store, run_ids: list[str]) -> None:
        """No timer outlives its closed run.

        Scoped to a simulation's own closed-world store (every execution in ``store``
        was created by this simulation, and this is only called once every tracked run
        has closed): the :class:`~loomwork.store.base.Store` contract has no per-run
        timer listing, but if every run is closed and a timer is still outstanding
        anywhere in the store, it can only belong to one of them.
        """
        if not run_ids:
            return
        remaining = await store.next_timer_at()
        if remaining is not None:
            self._fail(
                run_ids[0],
                "no_leftover_timers",
                f"a timer is still outstanding at {remaining!r} after every tracked run closed",
            )

    async def check_outbox_drained(self, store: Store, run_ids: list[str]) -> None:
        """The outbox eventually drains once every tracked run has closed."""
        if not run_ids:
            return
        size = await store.outbox_size()
        if size:
            self._fail(run_ids[0], "outbox_drained", f"{size} message(s) still queued")
