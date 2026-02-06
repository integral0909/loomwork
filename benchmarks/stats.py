"""Statistics for benchmark measurements: percentiles, confidence intervals and
regression detection.

Everything here is stdlib-only (``statistics`` and ``random``). Confidence intervals
are computed by bootstrap resampling rather than a closed-form formula so the same
machinery works for a throughput median and a latency percentile alike, without
assuming a distribution shape -- tick and step latencies are heavily right-skewed, and
a handful of trials is too few to lean on the central limit theorem.
"""

from __future__ import annotations

import statistics
from collections.abc import Callable, Sequence
from dataclasses import dataclass

DEFAULT_CONFIDENCE = 0.95
DEFAULT_RESAMPLES = 2000


def percentile(data: Sequence[float], p: float) -> float:
    """The ``p``-th percentile (0-100) of ``data`` by linear interpolation between the
    two nearest ranks. ``p=50`` is the median."""
    if not data:
        raise ValueError("percentile of an empty sequence is undefined")
    if not 0 <= p <= 100:
        raise ValueError(f"p must be between 0 and 100, got {p}")
    ordered = sorted(data)
    if len(ordered) == 1:
        return ordered[0]
    rank = (p / 100) * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def bootstrap_ci(
    data: Sequence[float],
    statistic: Callable[[Sequence[float]], float],
    *,
    confidence: float = DEFAULT_CONFIDENCE,
    resamples: int = DEFAULT_RESAMPLES,
    seed: int = 0,
) -> tuple[float, float]:
    """A ``confidence`` confidence interval for ``statistic(data)`` by resampling.

    With fewer than two observations there is nothing to resample; the interval
    collapses to the point estimate, which callers should treat as "unknown noise"
    rather than "no noise" -- see :func:`compare_points`.
    """
    if len(data) < 2:
        point = statistic(data) if data else 0.0
        return point, point
    rng_state = _splitmix64(seed)
    n = len(data)
    estimates = []
    for _ in range(resamples):
        sample = [data[_next_index(rng_state, n)] for _ in range(n)]
        estimates.append(statistic(sample))
    tail = (1 - confidence) / 2 * 100
    return percentile(estimates, tail), percentile(estimates, 100 - tail)


def _splitmix64(seed: int) -> list[int]:
    """A tiny, dependency-free PRNG state (splitmix64), boxed in a list so
    :func:`_next_index` can advance it in place without ``nonlocal``."""
    return [seed & 0xFFFFFFFFFFFFFFFF]


def _next_index(state: list[int], n: int) -> int:
    state[0] = (state[0] + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    z = state[0]
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    z = z ^ (z >> 31)
    return z % n


@dataclass(slots=True, frozen=True)
class LatencySummary:
    """Percentiles of a pool of latency samples (seconds), with a bootstrap confidence
    interval around the median -- the point :func:`compare_points` treats as the metric's
    representative value."""

    n: int
    median: float
    p50: float
    p95: float
    p99: float
    ci_low: float
    ci_high: float
    mean: float
    stdev: float


def summarize_latency(
    samples: Sequence[float],
    *,
    confidence: float = DEFAULT_CONFIDENCE,
    resamples: int = DEFAULT_RESAMPLES,
    seed: int = 0,
) -> LatencySummary:
    if not samples:
        raise ValueError("no latency samples to summarize")
    median = statistics.median(samples)
    ci_low, ci_high = bootstrap_ci(
        samples, statistics.median, confidence=confidence, resamples=resamples, seed=seed
    )
    return LatencySummary(
        n=len(samples),
        median=median,
        p50=percentile(samples, 50),
        p95=percentile(samples, 95),
        p99=percentile(samples, 99),
        ci_low=ci_low,
        ci_high=ci_high,
        mean=statistics.fmean(samples),
        stdev=statistics.pstdev(samples) if len(samples) > 1 else 0.0,
    )


@dataclass(slots=True, frozen=True)
class ThroughputSummary:
    """Per-trial throughput values (operations/second) summarized across trials, with a
    bootstrap confidence interval around the median."""

    n: int
    median: float
    mean: float
    stdev: float
    ci_low: float
    ci_high: float


def summarize_throughput(
    values: Sequence[float],
    *,
    confidence: float = DEFAULT_CONFIDENCE,
    resamples: int = DEFAULT_RESAMPLES,
    seed: int = 0,
) -> ThroughputSummary:
    if not values:
        raise ValueError("no throughput samples to summarize")
    ci_low, ci_high = bootstrap_ci(
        values, statistics.median, confidence=confidence, resamples=resamples, seed=seed
    )
    return ThroughputSummary(
        n=len(values),
        median=statistics.median(values),
        mean=statistics.fmean(values),
        stdev=statistics.pstdev(values) if len(values) > 1 else 0.0,
        ci_low=ci_low,
        ci_high=ci_high,
    )


@dataclass(slots=True, frozen=True)
class Verdict:
    """The result of comparing one metric between two result files."""

    delta_pct: float
    regressed: bool
    improved: bool
    noise: bool
    """The two confidence intervals overlap: whatever the point estimates say, the
    difference is not distinguishable from run-to-run noise at this sample size."""

    @property
    def changed(self) -> bool:
        return self.regressed or self.improved


def compare_points(
    old_point: float,
    old_ci: tuple[float, float],
    new_point: float,
    new_ci: tuple[float, float],
    *,
    threshold_pct: float,
    higher_is_better: bool,
) -> Verdict:
    """Whether ``new`` regresses, improves, or is indistinguishable from ``old``.

    A change only counts as a regression (or improvement) when it clears both bars:
    the point estimates differ by at least ``threshold_pct`` percent, *and* the two
    confidence intervals do not overlap. The second condition is what keeps a noisy but
    unchanged benchmark from being flagged -- a wide interval means the point estimate
    moved by chance, not because anything got slower.
    """
    if old_point == 0:
        delta_pct = 0.0 if new_point == 0 else float("inf")
    else:
        delta_pct = (new_point - old_point) / abs(old_point) * 100
    overlaps = new_ci[0] <= old_ci[1] and old_ci[0] <= new_ci[1]
    noise = overlaps or abs(delta_pct) < threshold_pct
    got_worse = delta_pct < 0 if higher_is_better else delta_pct > 0
    return Verdict(
        delta_pct=delta_pct,
        regressed=not noise and got_worse,
        improved=not noise and not got_worse,
        noise=noise,
    )
