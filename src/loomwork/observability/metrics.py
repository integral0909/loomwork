"""A small, dependency-free metrics registry with Prometheus text exposition.

Loomwork records its own operational metrics (ticks, step attempts, retries, schedule
firings, outbox throughput) through :class:`MetricsRegistry`. The registry renders the
Prometheus text format (version 0.0.4), so a worker or server can expose ``/metrics``
without extra dependencies; deployments that standardise on OpenTelemetry use the
adapter in :mod:`loomwork.observability.otel` instead or in addition.

Metric names follow Prometheus conventions: ``snake_case``, a ``loomwork_`` prefix, base
units in the name (``_seconds``), and ``_total`` for counters.
"""

from __future__ import annotations

import math
import re
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

#: Default buckets for durations: 1ms .. ~1min, roughly x2.5 per step.
DEFAULT_BUCKETS: tuple[float, ...] = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)

LabelValues = tuple[str, ...]


class MetricsError(ValueError):
    """A metric was defined or used incorrectly."""


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_number(value: float) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if math.isnan(value):
        return "NaN"
    if value == int(value) and abs(value) < 1e15:
        return str(int(value))
    return repr(value)


class _Metric:
    kind = ""

    def __init__(self, name: str, help_text: str, labels: Sequence[str]) -> None:
        if not _NAME_RE.match(name):
            raise MetricsError(f"invalid metric name {name!r}")
        for label in labels:
            if not _LABEL_RE.match(label) or label.startswith("__"):
                raise MetricsError(f"invalid label name {label!r}")
        if len(set(labels)) != len(labels):
            raise MetricsError(f"duplicate label names in {labels!r}")
        self.name = name
        self.help = help_text
        self.labels = tuple(labels)
        self._lock = threading.Lock()

    def _key(self, labels: Mapping[str, str]) -> LabelValues:
        if set(labels) != set(self.labels):
            raise MetricsError(
                f"{self.name} expects labels {sorted(self.labels)}, got {sorted(labels)}"
            )
        return tuple(str(labels[name]) for name in self.labels)

    def _label_text(self, values: LabelValues, extra: Mapping[str, str] | None = None) -> str:
        pairs = [f'{n}="{_escape(v)}"' for n, v in zip(self.labels, values, strict=True)]
        if extra:
            pairs.extend(f'{n}="{_escape(v)}"' for n, v in extra.items())
        return "{" + ",".join(pairs) + "}" if pairs else ""

    def header(self) -> list[str]:
        return [f"# HELP {self.name} {_escape(self.help)}", f"# TYPE {self.name} {self.kind}"]

    def render(self) -> list[str]:
        raise NotImplementedError


class Counter(_Metric):
    """A monotonically increasing value."""

    kind = "counter"

    def __init__(self, name: str, help_text: str, labels: Sequence[str] = ()) -> None:
        super().__init__(name, help_text, labels)
        self._values: dict[LabelValues, float] = {}

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        if amount < 0:
            raise MetricsError("counters can only increase")
        key = self._key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def value(self, **labels: str) -> float:
        return self._values.get(self._key(labels), 0.0)

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        return [f"{self.name}{self._label_text(k)} {_format_number(v)}" for k, v in items]


class Gauge(_Metric):
    """A value that can go up and down."""

    kind = "gauge"

    def __init__(self, name: str, help_text: str, labels: Sequence[str] = ()) -> None:
        super().__init__(name, help_text, labels)
        self._values: dict[LabelValues, float] = {}

    def set(self, value: float, **labels: str) -> None:
        with self._lock:
            self._values[self._key(labels)] = float(value)

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        key = self._key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def dec(self, amount: float = 1.0, **labels: str) -> None:
        self.inc(-amount, **labels)

    def value(self, **labels: str) -> float:
        return self._values.get(self._key(labels), 0.0)

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        return [f"{self.name}{self._label_text(k)} {_format_number(v)}" for k, v in items]


@dataclass(slots=True)
class _HistogramState:
    counts: list[int]
    total: float = 0.0
    count: int = 0


class Histogram(_Metric):
    """Observations counted into cumulative buckets (Prometheus semantics)."""

    kind = "histogram"

    def __init__(
        self,
        name: str,
        help_text: str,
        labels: Sequence[str] = (),
        buckets: Iterable[float] = DEFAULT_BUCKETS,
    ) -> None:
        super().__init__(name, help_text, labels)
        if "le" in self.labels:
            raise MetricsError("'le' is reserved for histogram buckets")
        bounds = sorted({float(b) for b in buckets})
        if not bounds:
            raise MetricsError("a histogram needs at least one bucket")
        if bounds[-1] != math.inf:
            bounds.append(math.inf)
        self.buckets = tuple(bounds)
        self._states: dict[LabelValues, _HistogramState] = {}

    def observe(self, value: float, **labels: str) -> None:
        key = self._key(labels)
        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = self._states[key] = _HistogramState([0] * len(self.buckets))
            for index, bound in enumerate(self.buckets):
                if value <= bound:
                    state.counts[index] += 1
                    break
            state.total += value
            state.count += 1

    def count(self, **labels: str) -> int:
        state = self._states.get(self._key(labels))
        return state.count if state else 0

    def sum(self, **labels: str) -> float:
        state = self._states.get(self._key(labels))
        return state.total if state else 0.0

    def render(self) -> list[str]:
        lines: list[str] = []
        with self._lock:
            items = sorted(self._states.items())
            for key, state in items:
                cumulative = 0
                for bound, count in zip(self.buckets, state.counts, strict=True):
                    cumulative += count
                    le = {"le": _format_number(bound)}
                    lines.append(f"{self.name}_bucket{self._label_text(key, le)} {cumulative}")
                lines.append(
                    f"{self.name}_sum{self._label_text(key)} {_format_number(state.total)}"
                )
                lines.append(f"{self.name}_count{self._label_text(key)} {state.count}")
        return lines


class MetricsRegistry:
    """Holds metrics and renders them in the Prometheus text format."""

    CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

    def __init__(self) -> None:
        self._metrics: dict[str, _Metric] = {}
        self._lock = threading.Lock()

    def _register(self, metric: _Metric) -> _Metric:
        with self._lock:
            existing = self._metrics.get(metric.name)
            if existing is not None:
                if type(existing) is not type(metric) or existing.labels != metric.labels:
                    raise MetricsError(f"metric {metric.name!r} is already registered differently")
                return existing
            self._metrics[metric.name] = metric
            return metric

    def counter(self, name: str, help_text: str, labels: Sequence[str] = ()) -> Counter:
        metric = self._register(Counter(name, help_text, labels))
        assert isinstance(metric, Counter)
        return metric

    def gauge(self, name: str, help_text: str, labels: Sequence[str] = ()) -> Gauge:
        metric = self._register(Gauge(name, help_text, labels))
        assert isinstance(metric, Gauge)
        return metric

    def histogram(
        self,
        name: str,
        help_text: str,
        labels: Sequence[str] = (),
        buckets: Iterable[float] = DEFAULT_BUCKETS,
    ) -> Histogram:
        metric = self._register(Histogram(name, help_text, labels, buckets))
        assert isinstance(metric, Histogram)
        return metric

    def get(self, name: str) -> _Metric | None:
        return self._metrics.get(name)

    def render(self) -> str:
        with self._lock:
            metrics = sorted(self._metrics.values(), key=lambda m: m.name)
        lines: list[str] = []
        for metric in metrics:
            lines.extend(metric.header())
            lines.extend(metric.render())
        return "\n".join(lines) + "\n"
