"""OpenTelemetry integration (traces and metrics).

Requires the ``otel`` extra (``opentelemetry-api``). Loomwork only uses the API; configure
providers and exporters (OTLP, Jaeger, Prometheus, ...) with the OpenTelemetry SDK in
your application as usual::

    from loomwork.observability import OpenTelemetryTelemetry
    app = Loomwork(..., telemetry=OpenTelemetryTelemetry())

Spans
    ``workflow.tick <workflow>``: one per tick, with its real start and end times.
    ``step <step>``: one per step attempt, made *current* while the step runs, so
    instrumented HTTP clients and database drivers inside the step become its children.

Metrics
    The same measurements as :class:`~loomwork.observability.MetricsTelemetry`, named
    with OpenTelemetry conventions (``loomwork.ticks``, ``loomwork.step.attempts``, ...).
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator
from typing import Any

from loomwork.observability.telemetry import AttemptInfo, AttemptReport, TickReport

_INSTRUMENTATION = "loomwork"


def _import_otel() -> tuple[Any, Any, Any]:
    try:
        from opentelemetry import context, metrics, trace
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "OpenTelemetryTelemetry requires 'opentelemetry-api': pip install 'loomwork[otel]'"
        ) from exc
    return trace, metrics, context


class OpenTelemetryTelemetry:
    """Reports Loomwork activity as OpenTelemetry spans and metrics."""

    def __init__(self, tracer_provider: Any = None, meter_provider: Any = None) -> None:
        trace, metrics, context = _import_otel()
        self._trace = trace
        self._context = context
        from loomwork._version import __version__

        self.tracer = trace.get_tracer(_INSTRUMENTATION, __version__, tracer_provider)
        meter = metrics.get_meter(_INSTRUMENTATION, __version__, meter_provider=meter_provider)
        self.ticks = meter.create_counter("loomwork.ticks", description="Workflow ticks processed")
        self.tick_duration = meter.create_histogram(
            "loomwork.tick.duration", unit="s", description="Time to process a workflow tick"
        )
        self.attempts = meter.create_counter(
            "loomwork.step.attempts", description="Step attempts by outcome"
        )
        self.attempt_duration = meter.create_histogram(
            "loomwork.step.duration", unit="s", description="Duration of step attempts"
        )
        self.queue_delay = meter.create_histogram(
            "loomwork.step.queue_delay", unit="s", description="Time a ready step job waited"
        )
        self.schedule_runs = meter.create_counter(
            "loomwork.schedule.runs", description="Runs started or skipped by schedules"
        )
        self.outbox = meter.create_counter(
            "loomwork.outbox.messages", description="Cross-run messages relayed"
        )
        self.errors = meter.create_counter(
            "loomwork.worker.errors", description="Unexpected errors in worker loops"
        )
        self._open_spans: dict[tuple[str, int, int], Any] = {}

    # ------------------------------------------------------------------ ticks

    def tick_finished(self, report: TickReport) -> None:
        end = time.time_ns()
        start = end - int(report.duration * 1e9)
        span = self.tracer.start_span(
            f"workflow.tick {report.workflow_name}",
            start_time=start,
            attributes={
                "loomwork.run_id": report.run_id,
                "loomwork.workflow": report.workflow_name,
                "loomwork.tick.outcome": report.outcome,
                "loomwork.tick.commands": report.commands,
                "loomwork.tick.replayed": report.replayed_ticks,
                "loomwork.tick.cache_hit": report.cache_hit,
            },
        )
        if report.outcome == "failed":
            span.set_status(self._trace.Status(self._trace.StatusCode.ERROR, report.error or ""))
        span.end(end_time=end)
        attributes = {"workflow": report.workflow_name, "outcome": report.outcome}
        self.ticks.add(1, attributes)
        self.tick_duration.record(report.duration, {"workflow": report.workflow_name})

    # ------------------------------------------------------------------ steps

    @contextlib.contextmanager
    def step_attempt(self, info: AttemptInfo) -> Iterator[None]:
        span = self.tracer.start_span(
            f"step {info.step_name}",
            attributes={
                "loomwork.run_id": info.run_id,
                "loomwork.workflow": info.workflow_name,
                "loomwork.step": info.step_name,
                "loomwork.step.seq": info.seq,
                "loomwork.step.attempt": info.attempt,
                "loomwork.queue": info.queue,
            },
        )
        key = (info.run_id, info.seq, info.attempt)
        self._open_spans[key] = span
        self.queue_delay.record(max(info.queue_delay, 0.0), {"queue": info.queue})
        token = self._context.attach(self._trace.set_span_in_context(span))
        try:
            yield
        except BaseException:
            # Interrupted (for example by shutdown): no outcome will be reported.
            if self._open_spans.pop(key, None) is not None:
                span.end()
            raise
        finally:
            self._context.detach(token)
        # On normal exit step_attempt_finished ends the span with the attempt's outcome.

    def step_attempt_finished(self, report: AttemptReport) -> None:
        info = report.info
        span = self._open_spans.pop((info.run_id, info.seq, info.attempt), None)
        if span is not None:
            span.set_attribute("loomwork.step.outcome", report.outcome)
            if report.failure_type:
                span.set_attribute("loomwork.step.failure_type", report.failure_type)
            if report.outcome in {"failed", "retrying"}:
                span.set_status(
                    self._trace.Status(self._trace.StatusCode.ERROR, report.failure_type or "")
                )
            span.end()
        attributes = {"step": info.step_name, "queue": info.queue}
        self.attempts.add(1, {**attributes, "outcome": report.outcome})
        self.attempt_duration.record(report.duration, attributes)

    # ------------------------------------------------------------------ other

    def schedule_evaluated(self, schedule_id: str, started: int, skipped: int) -> None:
        if started:
            self.schedule_runs.add(started, {"result": "started"})
        if skipped:
            self.schedule_runs.add(skipped, {"result": "skipped"})

    def outbox_relayed(self, count: int) -> None:
        if count:
            self.outbox.add(count)

    def worker_error(self, loop: str) -> None:
        self.errors.add(1, {"loop": loop})

    def close_open_spans(self) -> None:
        """End spans of attempts that never reported (e.g. on shutdown)."""
        for span in self._open_spans.values():
            span.end()
        self._open_spans.clear()
