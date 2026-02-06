"""Percentiles, confidence intervals, and regression detection.

The regression-detection tests are the point of this module: a real benchmark run is
noisy, and :func:`~benchmarks.stats.compare_points` must not cry wolf over noise that
stays inside the confidence interval, only over a change that clears both the
threshold and the interval.
"""

from __future__ import annotations

import pytest

from benchmarks import stats


class TestPercentile:
    def test_median_of_four(self) -> None:
        assert stats.percentile([1.0, 2.0, 3.0, 4.0], 50) == pytest.approx(2.5)

    def test_p0_is_min_and_p100_is_max(self) -> None:
        data = [5.0, 1.0, 9.0, 3.0]
        assert stats.percentile(data, 0) == 1.0
        assert stats.percentile(data, 100) == 9.0

    def test_single_value(self) -> None:
        assert stats.percentile([7.0], 50) == 7.0

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            stats.percentile([], 50)

    def test_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 100"):
            stats.percentile([1.0, 2.0], 101)


class TestBootstrapCi:
    def test_single_or_no_sample_returns_point_twice(self) -> None:
        assert stats.bootstrap_ci([], lambda d: 0.0, seed=1) == (0.0, 0.0)
        assert stats.bootstrap_ci([5.0], lambda d: d[0], seed=1) == (5.0, 5.0)

    def test_bounds_the_point_estimate(self) -> None:
        import statistics

        data = [10.0, 12.0, 9.0, 11.0, 10.5, 9.5, 10.0, 11.5, 9.0, 10.0]
        low, high = stats.bootstrap_ci(data, statistics.median, seed=7)
        assert low <= statistics.median(data) <= high

    def test_deterministic_for_a_fixed_seed(self) -> None:
        import statistics

        data = [1.0, 5.0, 3.0, 9.0, 2.0, 7.0]
        first = stats.bootstrap_ci(data, statistics.median, seed=123)
        second = stats.bootstrap_ci(data, statistics.median, seed=123)
        assert first == second


class TestSummarizeLatency:
    def test_percentiles_are_ordered(self) -> None:
        data = [0.001 * i for i in range(1, 101)]
        summary = stats.summarize_latency(data, seed=1)
        assert summary.p50 <= summary.p95 <= summary.p99
        assert summary.ci_low <= summary.median <= summary.ci_high
        assert summary.n == 100

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="no latency samples"):
            stats.summarize_latency([], seed=1)


class TestSummarizeThroughput:
    def test_basic_shape(self) -> None:
        summary = stats.summarize_throughput([100.0, 102.0, 98.0, 101.0, 99.0], seed=1)
        assert summary.n == 5
        assert summary.ci_low <= summary.median <= summary.ci_high

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="no throughput samples"):
            stats.summarize_throughput([], seed=1)


class TestComparePoints:
    """Direct tests of the comparison rule: a threshold crossing *and* a
    non-overlapping confidence interval, in the direction that means "worse"."""

    def test_clear_throughput_regression(self) -> None:
        verdict = stats.compare_points(
            100.0, (98.0, 102.0), 70.0, (68.0, 72.0), threshold_pct=10.0, higher_is_better=True
        )
        assert verdict.regressed
        assert not verdict.improved
        assert not verdict.noise
        assert verdict.delta_pct == pytest.approx(-30.0)

    def test_clear_throughput_improvement(self) -> None:
        verdict = stats.compare_points(
            100.0, (98.0, 102.0), 130.0, (128.0, 132.0), threshold_pct=10.0, higher_is_better=True
        )
        assert verdict.improved
        assert not verdict.regressed

    def test_latency_increase_is_a_regression(self) -> None:
        # Higher is worse for a latency metric: growing from 1ms to 1.5ms regresses.
        verdict = stats.compare_points(
            0.001,
            (0.00098, 0.00102),
            0.0015,
            (0.00148, 0.00152),
            threshold_pct=10.0,
            higher_is_better=False,
        )
        assert verdict.regressed

    def test_latency_decrease_is_an_improvement(self) -> None:
        verdict = stats.compare_points(
            0.0015,
            (0.00148, 0.00152),
            0.001,
            (0.00098, 0.00102),
            threshold_pct=10.0,
            higher_is_better=False,
        )
        assert verdict.improved

    def test_small_delta_is_noise_even_without_ci_overlap(self) -> None:
        # 2% apart and tightly measured: below the threshold, so not a regression
        # regardless of how tight the intervals are.
        verdict = stats.compare_points(
            100.0, (99.9, 100.1), 98.0, (97.9, 98.1), threshold_pct=10.0, higher_is_better=True
        )
        assert verdict.noise
        assert not verdict.regressed
        assert not verdict.improved

    def test_large_apparent_delta_with_overlapping_ci_is_noise(self) -> None:
        # A 20% difference in point estimates, but wide, overlapping confidence
        # intervals: exactly the "noisy but unchanged" case that must not be flagged.
        verdict = stats.compare_points(
            100.0, (80.0, 120.0), 120.0, (90.0, 150.0), threshold_pct=10.0, higher_is_better=True
        )
        assert verdict.noise
        assert not verdict.regressed
        assert not verdict.improved

    def test_zero_baseline_with_no_change(self) -> None:
        verdict = stats.compare_points(
            0.0, (0.0, 0.0), 0.0, (0.0, 0.0), threshold_pct=10.0, higher_is_better=True
        )
        assert verdict.delta_pct == 0.0
        assert verdict.noise


class TestComparePointsEndToEnd:
    """The full pipeline (raw samples -> summary -> compare_points) on synthetic data
    resembling two independent trial runs of the same, unchanged benchmark."""

    def test_noisy_but_unchanged_throughput_is_not_flagged(self) -> None:
        # Same center (~100), different noise: two independent measurements of a
        # benchmark that did not actually change.
        old = [98, 102, 95, 105, 100, 97, 103, 99, 101, 96, 104, 100, 98, 102, 94, 106, 100, 99]
        new = [101, 99, 104, 96, 100, 103, 97, 102, 98, 105, 95, 100, 102, 98, 106, 94, 99, 101]
        old_summary = stats.summarize_throughput([float(v) for v in old], seed=1)
        new_summary = stats.summarize_throughput([float(v) for v in new], seed=2)
        verdict = stats.compare_points(
            old_summary.median,
            (old_summary.ci_low, old_summary.ci_high),
            new_summary.median,
            (new_summary.ci_low, new_summary.ci_high),
            threshold_pct=5.0,
            higher_is_better=True,
        )
        assert not verdict.regressed
        assert not verdict.improved

    def test_consistent_latency_regression_is_flagged(self) -> None:
        old = [0.0010, 0.0011, 0.0009, 0.0010, 0.0011, 0.0009, 0.0010, 0.0010]
        new = [0.0016, 0.0017, 0.0015, 0.0016, 0.0017, 0.0015, 0.0016, 0.0016]
        old_summary = stats.summarize_latency(old, seed=1)
        new_summary = stats.summarize_latency(new, seed=2)
        verdict = stats.compare_points(
            old_summary.median,
            (old_summary.ci_low, old_summary.ci_high),
            new_summary.median,
            (new_summary.ci_low, new_summary.ci_high),
            threshold_pct=10.0,
            higher_is_better=False,
        )
        assert verdict.regressed
