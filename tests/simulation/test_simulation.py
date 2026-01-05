"""Runs the deterministic simulation over a handful of seeds.

The default suite here is deliberately small (a handful of seeds, few runs each) so the
quick loop (``pytest -m "not slow"``) stays fast; ``test_soak`` exercises a much larger
seed range under the ``simulation``/``slow`` markers, for the same soak coverage the
``python -m loomwork.testing.simulation`` CLI gives outside of pytest.
"""

from __future__ import annotations

import pytest

from loomwork.testing.simulation import SimulationConfig, SimulationReport, simulate
from loomwork.testing.simulation.__main__ import main as simulation_main

pytestmark = pytest.mark.integration

_QUICK_CONFIG = SimulationConfig(
    num_runs=8,
    num_workers=3,
    fault_rate=0.15,
    lease_pressure=0.1,
    kill_rate=0.4,
    cache_evict_rate=0.4,
    wall_clock_timeout=10.0,
)


@pytest.mark.parametrize("seed", range(1, 9))
def test_simulation_seeds(seed: int) -> None:
    """A handful of seeds, each exercising every workload, faults and worker chaos."""
    report = simulate(seed, _QUICK_CONFIG)
    assert report.ok, report.format_text()
    assert report.started == _QUICK_CONFIG.num_runs
    assert report.closed == report.started


def test_simulation_is_reproducible() -> None:
    """The same seed and config always script the same runs and settle every one of them
    on the same outcome -- the guarantee that matters: a seed that finds a violation
    finds the *same* violation again.

    What this does not check (see ``docs/simulation-testing.md``): the exact number of
    faults injected, or the order two independently-driven runs happen to finish in.
    Both depend on real ids the engine generates (48 bits of wall-clock time plus random
    bits, see ``loomwork.ids``) breaking ties between simultaneously-ready work, and on
    the worker's own poll timers, which this harness does not and cannot drive from the
    seed. ``run_id`` is one of those real ids, so it is deliberately excluded below.
    """
    first = simulate(4321, _QUICK_CONFIG)
    second = simulate(4321, _QUICK_CONFIG)

    def outcomes(report: SimulationReport) -> list[tuple[str, tuple[object, ...], str | None]]:
        return sorted((r.workload, r.plan.args, r.outcome) for r in report.runs)

    assert outcomes(first) == outcomes(second)


def test_simulation_reports_a_failing_run_without_crashing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A workload whose expected outcome does not match reality is a controlled
    violation, not an exception escaping ``simulate`` -- the property the whole harness
    depends on to survive a real bug instead of crashing on the first one it finds."""
    import loomwork as lw
    from loomwork.testing.simulation import harness as harness_module
    from loomwork.testing.simulation.workloads import (
        ExpectedOutcome,
        RunPlan,
        SideEffectLog,
        Workload,
    )

    @lw.workflow(name="sim.test.always_completes")
    async def always_completes(run_key: str) -> str:
        return run_key

    def poisoned(log: SideEffectLog, *, names: frozenset[str] | None = None) -> list[Workload]:
        def plan(rng: object, index: int) -> RunPlan:
            return RunPlan(
                workload="poisoned",
                args=(f"poisoned-{index}",),
                expected=ExpectedOutcome(status="completed", result="not what it returns"),
            )

        return [
            Workload(
                name="poisoned",
                entry=always_completes,
                workflows=(always_completes,),
                steps=(),
                plan=plan,
            )
        ]

    monkeypatch.setattr(harness_module, "build_workloads", poisoned)
    report = simulate(1, SimulationConfig(num_runs=1, num_workers=1, fault_rate=0.0))
    assert not report.ok
    assert any(v.check == "expected_outcome" for v in report.violations)
    assert "not what it returns" in report.format_text()


def test_cli_main_runs() -> None:
    exit_code = simulation_main(["--seeds", "1..2", "--num-runs", "4", "--quiet"])
    assert exit_code == 0


@pytest.mark.slow
@pytest.mark.simulation
def test_soak() -> None:
    """The same coverage as the CLI's soak mode, at a size the slow marker exists for."""
    config = SimulationConfig(num_runs=10, num_workers=3, fault_rate=0.1)
    failures = []
    for seed in range(1, 51):
        report = simulate(seed, config)
        if not report.ok:
            failures.append(report)
    assert not failures, "\n\n".join(f.format_text() for f in failures)


@pytest.mark.slow
def test_sqlite_backend() -> None:
    """The same harness against a real embedded database, not just MemoryStore.

    Deliberately gentler fault and chaos rates than the memory-backed tests: there is no
    clock to skip here (see docs/simulation-testing.md), so every retry and every worker
    restart costs real time. A soak run against SQLite should size these down from the
    (memory-tuned) defaults; see the CLI's --fault-rate/--kill-rate/--cache-evict-rate
    flags.
    """
    config = SimulationConfig(
        store="sqlite",
        num_runs=6,
        num_workers=2,
        fault_rate=0.03,
        kill_rate=0.05,
        cache_evict_rate=0.05,
        wall_clock_timeout=20.0,
    )
    report = simulate(1, config)
    assert report.ok, report.format_text()
