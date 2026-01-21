"""Telemetry hooks reported by a real worker."""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import loomwork as lw
from loomwork import ManualClock, MemoryStore
from loomwork.observability import (
    CompositeTelemetry,
    LoggingTelemetry,
    MetricsRegistry,
    MetricsTelemetry,
    OpenTelemetryTelemetry,
    Telemetry,
)
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration

attempts: dict[str, int] = {}
seen_trace_ids: list[int] = []


@lw.step(name="obs.flaky", retry=lw.RetryPolicy(initial_interval=0.01, max_attempts=3, jitter=0))
async def flaky(key: str) -> str:
    attempts[key] = attempts.get(key, 0) + 1
    span = trace.get_current_span()
    seen_trace_ids.append(span.get_span_context().trace_id)
    if attempts[key] == 1:
        raise ConnectionError("first attempt fails")
    return key


@lw.workflow(name="obs.flow")
async def flow(key: str) -> str:
    result = await flaky(key)
    await lw.sleep(timedelta(seconds=1))
    return result


async def run_flow(clock: ManualClock, telemetry: object, key: str) -> None:
    attempts.pop(key, None)
    app = lw.Loomwork(
        store=MemoryStore(),
        workflows=[flow],
        steps=[flaky],
        clock=clock,
        config=FAST_CONFIG,
        worker_options=WorkerOptions(shutdown_grace=0.2),
        telemetry=telemetry,  # type: ignore[arg-type]
    )
    async with app:
        handle = await app.start_workflow(flow, key)
        await eventually(lambda: app.store.timer_count == 1, clock=clock, advance=0.02)  # type: ignore[attr-defined]
        await clock.advance(1)
        assert await handle.result(timeout=5) == key


async def test_metrics_telemetry(clock: ManualClock) -> None:
    registry = MetricsRegistry()
    await run_flow(clock, MetricsTelemetry(registry), "m")
    text = registry.render()
    # Labels render in declaration order.
    assert (
        'loomwork_step_attempts_total{step="obs.flaky",queue="default",outcome="retrying"} 1'
        in text
    )
    assert (
        'loomwork_step_attempts_total{step="obs.flaky",queue="default",outcome="completed"} 1'
        in text
    )
    assert 'loomwork_ticks_total{workflow="obs.flow",outcome="completed"} 1' in text
    assert 'loomwork_ticks_total{workflow="obs.flow",outcome="progressed"}' in text
    assert "loomwork_tick_duration_seconds_bucket" in text
    assert 'loomwork_step_attempts_running{queue="default"} 0' in text
    assert 'loomwork_replay_cache_total{result="hit"}' in text


async def test_opentelemetry_spans_and_metrics(clock: ManualClock) -> None:
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    seen_trace_ids.clear()

    telemetry = OpenTelemetryTelemetry(
        tracer_provider=tracer_provider, meter_provider=meter_provider
    )
    await run_flow(clock, telemetry, "o")

    spans = exporter.get_finished_spans()
    step_spans = [s for s in spans if s.name == "step obs.flaky"]
    assert len(step_spans) == 2
    assert [(s.attributes or {}).get("loomwork.step.outcome") for s in step_spans] == [
        "retrying",
        "completed",
    ]
    assert step_spans[0].status.status_code is trace.StatusCode.ERROR
    # The step span is current while the step's own code runs.
    assert seen_trace_ids == [s.context.trace_id for s in step_spans if s.context is not None]
    tick_spans = [s for s in spans if s.name == "workflow.tick obs.flow"]
    assert len(tick_spans) >= 3
    assert all((s.end_time or 0) >= (s.start_time or 0) for s in tick_spans)

    data = reader.get_metrics_data()
    names = {
        metric.name
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    assert {"loomwork.ticks", "loomwork.step.attempts", "loomwork.step.duration"} <= names


async def test_logging_and_composite(clock: ManualClock, caplog: pytest.LogCaptureFixture) -> None:
    registry = MetricsRegistry()
    composite = CompositeTelemetry([LoggingTelemetry(), MetricsTelemetry(registry)])
    with caplog.at_level(logging.DEBUG, logger="loomwork.telemetry"):
        await run_flow(clock, composite, "c")
    records = [r for r in caplog.records if r.name == "loomwork.telemetry"]
    assert any(getattr(r, "outcome", None) == "retrying" for r in records)
    assert any(getattr(r, "step_name", None) == "obs.flaky" for r in records)
    assert "loomwork_step_attempts_total" in registry.render()


def test_opentelemetry_reports_history_and_build_signals() -> None:
    reader = InMemoryMetricReader()
    # Assigning to the protocol type makes mypy check that no hook is missing.
    telemetry: Telemetry = OpenTelemetryTelemetry(
        tracer_provider=TracerProvider(), meter_provider=MeterProvider(metric_readers=[reader])
    )
    telemetry.history_size_warning("run-1", "orders.flow", 10_500, "events")
    telemetry.pinned_runs_waiting("2024.10.1", 3)
    telemetry.pinned_runs_waiting("2024.10.1", 2)

    points = {
        metric.name: list(metric.data.data_points)
        for resource in reader.get_metrics_data().resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    (warning,) = points["loomwork.history.size_warnings"]
    assert warning.value == 1
    assert dict(warning.attributes or {}) == {"workflow": "orders.flow", "unit": "events"}
    (waiting,) = points["loomwork.build.pinned_runs_waiting"]
    assert waiting.value == 2
    assert dict(waiting.attributes or {}) == {"build_id": "2024.10.1"}
