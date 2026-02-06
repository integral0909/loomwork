"""The scenario framework.

A :class:`Scenario` describes *what* to measure, parameterised over one or more
:class:`Case` objects (a fan-out width, a payload size, a store backend...). It knows
nothing about statistics or reporting: :meth:`Scenario.run_trial` runs one trial and
returns raw numbers, and :func:`run_case` turns a warmup-then-repeat sequence of trials
into a :class:`~benchmarks.results.ScenarioResult` using :mod:`benchmarks.stats`.

Seeds are derived deterministically from a run's base seed plus the scenario name, case
label and trial index (see :func:`derive_seed`), so a ``--seed`` given to ``run``
reproduces the same workload data (payload bytes, RNG draws) on every run, even though
wall-clock timings themselves never repeat exactly.
"""

from __future__ import annotations

import hashlib
import statistics
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from benchmarks import stats
from benchmarks.results import Metric, ScenarioResult

StoreKind = Literal["memory", "sqlite", "postgres", "http"]

ALL_STORE_KINDS: tuple[StoreKind, ...] = ("memory", "sqlite", "postgres", "http")


def derive_seed(base_seed: int, *parts: str | int) -> int:
    """A deterministic 63-bit seed derived from ``base_seed`` and ``parts``."""
    digest = hashlib.sha256(":".join(str(p) for p in (base_seed, *parts)).encode()).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


@dataclass(slots=True, frozen=True)
class Case:
    """One parameterised instance of a scenario.

    ``trials`` is how many measured repetitions feed the reported statistics;
    ``warmup`` trials run first and are discarded (JIT-ish warmup: connection pools,
    replay caches, file system caches).
    """

    label: str
    params: dict[str, Any]
    trials: int = 5
    warmup: int = 1


@dataclass(slots=True)
class Trial:
    """The raw measurement one call to :meth:`Scenario.run_trial` produced.

    ``operations``/``seconds`` feed a throughput metric (operations per second);
    ``samples`` (seconds) feed a latency metric. A scenario only needs to fill in
    whichever its :attr:`Scenario.metric` uses. ``extra`` carries scenario-specific
    scalars (cache hit rate, claim contention counts, ...) that are reported but not
    compared across runs.
    """

    operations: int = 0
    seconds: float = 0.0
    samples: list[float] = field(default_factory=list)
    extra: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class BenchContext:
    """Everything a scenario needs to run one trial against one store backend."""

    store_kind: StoreKind
    quick: bool
    seed: int
    work_dir: Path
    postgres_dsn: str | None


class Scenario(ABC):
    """A named, parameterised thing to measure.

    Subclasses set the class attributes and implement :meth:`cases` and
    :meth:`run_trial`; everything else (warmup, repetition, statistics) is handled by
    :func:`run_case`.
    """

    name: str
    description: str
    metric: Metric
    unit: str
    #: Store backends this scenario can run against. Store micro-benchmarks that are
    #: only meaningful for a durable backend override this to exclude ``memory``.
    stores: tuple[StoreKind, ...] = ALL_STORE_KINDS

    def supports(self, store_kind: StoreKind) -> bool:
        return store_kind in self.stores

    @abstractmethod
    def cases(self, *, quick: bool) -> Sequence[Case]:
        """The parameter sets to measure. ``quick`` asks for a cheap subset."""

    @abstractmethod
    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        """Run one trial (one warmup or one measured repetition) and return raw numbers."""


def _merge_extra(trials: Sequence[Trial]) -> dict[str, float]:
    keys = {k for trial in trials for k in trial.extra}
    return {
        key: statistics.fmean(trial.extra[key] for trial in trials if key in trial.extra)
        for key in keys
    }


async def run_case(
    scenario: Scenario,
    store_kind: StoreKind,
    case: Case,
    *,
    base_seed: int,
    quick: bool,
    work_dir: Path,
    postgres_dsn: str | None,
) -> ScenarioResult:
    """Warm up, run ``case.trials`` measured trials, and summarize them."""
    for i in range(case.warmup):
        ctx = BenchContext(
            store_kind,
            quick,
            derive_seed(base_seed, scenario.name, case.label, "warmup", i),
            work_dir,
            postgres_dsn,
        )
        await scenario.run_trial(ctx, case)

    trials: list[Trial] = []
    for i in range(case.trials):
        ctx = BenchContext(
            store_kind,
            quick,
            derive_seed(base_seed, scenario.name, case.label, "trial", i),
            work_dir,
            postgres_dsn,
        )
        trials.append(await scenario.run_trial(ctx, case))

    extra = _merge_extra(trials)
    if scenario.metric == "throughput":
        throughput_summary = stats.summarize_throughput(
            [t.operations / t.seconds for t in trials], seed=base_seed
        )
        return ScenarioResult(
            scenario=scenario.name,
            description=scenario.description,
            store=store_kind,
            case=case.label,
            params=case.params,
            metric="throughput",
            unit=scenario.unit,
            trials=case.trials,
            warmup=case.warmup,
            n=throughput_summary.n,
            point=throughput_summary.median,
            ci_low=throughput_summary.ci_low,
            ci_high=throughput_summary.ci_high,
            mean=throughput_summary.mean,
            stdev=throughput_summary.stdev,
            extra=extra,
        )

    samples = [sample for trial in trials for sample in trial.samples]
    latency_summary = stats.summarize_latency(samples, seed=base_seed)
    return ScenarioResult(
        scenario=scenario.name,
        description=scenario.description,
        store=store_kind,
        case=case.label,
        params=case.params,
        metric="latency",
        unit=scenario.unit,
        trials=case.trials,
        warmup=case.warmup,
        n=latency_summary.n,
        point=latency_summary.median,
        ci_low=latency_summary.ci_low,
        ci_high=latency_summary.ci_high,
        mean=latency_summary.mean,
        stdev=latency_summary.stdev,
        p50=latency_summary.p50,
        p95=latency_summary.p95,
        p99=latency_summary.p99,
        extra=extra,
    )
