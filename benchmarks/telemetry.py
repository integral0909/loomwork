"""A :class:`~loomwork.observability.telemetry.Telemetry` that records raw samples
instead of aggregating them into metrics -- exactly what latency-type scenarios need
(tick duration by cache hit/miss, step dispatch delay) that
:class:`~loomwork.observability.telemetry.MetricsTelemetry` does not expose.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator

from loomwork.observability.telemetry import AttemptInfo, AttemptReport, NoopTelemetry, TickReport


class RecordingTelemetry(NoopTelemetry):
    """Collects every tick and step-attempt report seen while it is attached to a
    :class:`~loomwork.Loomwork` app. Safe to read after the app has stopped."""

    def __init__(self) -> None:
        self.tick_durations: list[float] = []
        self.tick_durations_cache_hit: list[float] = []
        self.tick_durations_cache_miss: list[float] = []
        self.queue_delays: list[float] = []
        self.step_durations: list[float] = []

    def tick_finished(self, report: TickReport) -> None:
        self.tick_durations.append(report.duration)
        bucket = (
            self.tick_durations_cache_hit if report.cache_hit else self.tick_durations_cache_miss
        )
        bucket.append(report.duration)

    @contextlib.contextmanager
    def step_attempt(self, info: AttemptInfo) -> Iterator[None]:
        self.queue_delays.append(info.queue_delay)
        yield

    def step_attempt_finished(self, report: AttemptReport) -> None:
        self.step_durations.append(report.duration)
