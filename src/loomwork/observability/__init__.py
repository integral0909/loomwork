"""Metrics, tracing and structured logging for Loomwork."""

from typing import TYPE_CHECKING

from loomwork.observability.exporter import MetricsServer
from loomwork.observability.metrics import (
    Counter,
    Gauge,
    Histogram,
    MetricsError,
    MetricsRegistry,
)
from loomwork.observability.telemetry import (
    AttemptInfo,
    AttemptReport,
    CompositeTelemetry,
    LoggingTelemetry,
    MetricsTelemetry,
    NoopTelemetry,
    Telemetry,
    TickReport,
)

if TYPE_CHECKING:
    from loomwork.observability.otel import OpenTelemetryTelemetry

__all__ = [
    "AttemptInfo",
    "AttemptReport",
    "CompositeTelemetry",
    "Counter",
    "Gauge",
    "Histogram",
    "LoggingTelemetry",
    "MetricsError",
    "MetricsRegistry",
    "MetricsServer",
    "MetricsTelemetry",
    "NoopTelemetry",
    "OpenTelemetryTelemetry",
    "Telemetry",
    "TickReport",
]


def __getattr__(name: str) -> object:
    # Imported lazily so that `opentelemetry` is only needed when actually used.
    if name == "OpenTelemetryTelemetry":
        from loomwork.observability.otel import OpenTelemetryTelemetry

        return OpenTelemetryTelemetry
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
