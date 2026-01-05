"""Deterministic simulation testing for Loomwork.

Runs many workflows through real workers under a seeded, fully reproducible schedule of
store faults, worker chaos and timing, checking the engine's invariants after every step.
Given the same seed, a simulation reproduces the exact same sequence of faults, chaos
events and workload arguments, so a failing seed is a reliable bug report::

    from loomwork.testing.simulation import SimulationConfig, simulate

    report = simulate(seed=1234, config=SimulationConfig(num_runs=50))
    if not report.ok:
        print(report.format_text())

See ``docs/simulation-testing.md`` for what it checks, how to reproduce a failing seed,
how to add a workload, and precisely what "reproducible" does and does not cover.
"""

from __future__ import annotations

from loomwork.testing.simulation.chaos import ChaosEvent, WorkerChaos
from loomwork.testing.simulation.faults import (
    FaultConfig,
    FaultEvent,
    FaultRates,
    FaultyStore,
    InjectedFault,
)
from loomwork.testing.simulation.harness import (
    RunRecord,
    SimulationConfig,
    SimulationReport,
    simulate,
)
from loomwork.testing.simulation.invariants import InvariantChecker, Violation
from loomwork.testing.simulation.loop import SimEventLoop, run_simulated
from loomwork.testing.simulation.workloads import (
    DriveHandle,
    ExpectedOutcome,
    RunPlan,
    SideEffectLog,
    Workload,
    build_workloads,
)

__all__ = [
    "ChaosEvent",
    "DriveHandle",
    "ExpectedOutcome",
    "FaultConfig",
    "FaultEvent",
    "FaultRates",
    "FaultyStore",
    "InjectedFault",
    "InvariantChecker",
    "RunPlan",
    "RunRecord",
    "SideEffectLog",
    "SimEventLoop",
    "SimulationConfig",
    "SimulationReport",
    "Violation",
    "WorkerChaos",
    "Workload",
    "build_workloads",
    "run_simulated",
    "simulate",
]
