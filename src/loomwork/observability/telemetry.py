"""Hooks through which the engine reports what it does.

Engine components call a :class:`Telemetry` object at meaningful points (a tick finished,
a step attempt failed, a schedule fired). Implementations turn those calls into metrics,
traces or logs. Hooks are synchronous and must be cheap; anything slow belongs in an
exporter running elsewhere.

The step-attempt hook is a context manager so tracing implementations can wrap the user's
step code in a span (and make that span current, so HTTP clients inside the step propagate
the trace).
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol

from loomwork.observability.metrics import MetricsRegistry


@dataclass(frozen=True, slots=True)
class TickReport:
    run_id: str
    workflow_name: str
    duration: float
    outcome: str  # "completed", "closed", "failed", "lease_lost", "empty"
    commands: int = 0
    replayed_ticks: int = 0
    cache_hit: bool = False
    error: str | None = None


@dataclass(frozen=True, slots=True)
class AttemptInfo:
    run_id: str
    workflow_name: str
    step_name: str
    queue: str
    attempt: int
    seq: int
    queue_delay: float
    """Seconds between the job becoming ready and the attempt starting."""


@dataclass(frozen=True, slots=True)
class AttemptReport:
    info: AttemptInfo
    duration: float
    outcome: str  # "completed", "retrying", "failed", "cancelled", "released", "lease_lost"
    failure_type: str | None = None


class Telemetry(Protocol):
    def tick_finished(self, report: TickReport) -> None: ...

    def step_attempt(self, info: AttemptInfo) -> contextlib.AbstractContextManager[None]:
        """Wraps the execution of one attempt."""
        ...

    def step_attempt_finished(self, report: AttemptReport) -> None: ...

    def schedule_evaluated(self, schedule_id: str, started: int, skipped: int) -> None: ...

    def outbox_relayed(self, count: int) -> None: ...

    def worker_error(self, loop: str) -> None: ...

    def history_size_warning(self, run_id: str, workflow_name: str, size: int, unit: str) -> None:
        """A run's history crossed ``history_warning_events`` or ``history_warning_bytes``.

        Reported at most once per run per ``unit`` (``"events"`` or ``"bytes"``), at the
        tick where the threshold was first crossed. Consider continue-as-new.
        """
        ...


class NoopTelemetry:
    """Records nothing. The default."""

    def tick_finished(self, report: TickReport) -> None:
        return None

    @contextlib.contextmanager
    def step_attempt(self, info: AttemptInfo) -> Iterator[None]:
        yield

    def step_attempt_finished(self, report: AttemptReport) -> None:
        return None

    def schedule_evaluated(self, schedule_id: str, started: int, skipped: int) -> None:
        return None

    def outbox_relayed(self, count: int) -> None:
        return None

    def worker_error(self, loop: str) -> None:
        return None

    def history_size_warning(self, run_id: str, workflow_name: str, size: int, unit: str) -> None:
        return None


class CompositeTelemetry:
    """Fans every hook out to several implementations."""

    def __init__(self, parts: Sequence[Telemetry]) -> None:
        self.parts = list(parts)

    def tick_finished(self, report: TickReport) -> None:
        for part in self.parts:
            part.tick_finished(report)

    @contextlib.contextmanager
    def step_attempt(self, info: AttemptInfo) -> Iterator[None]:
        with contextlib.ExitStack() as stack:
            for part in self.parts:
                stack.enter_context(part.step_attempt(info))
            yield

    def step_attempt_finished(self, report: AttemptReport) -> None:
        for part in self.parts:
            part.step_attempt_finished(report)

    def schedule_evaluated(self, schedule_id: str, started: int, skipped: int) -> None:
        for part in self.parts:
            part.schedule_evaluated(schedule_id, started, skipped)

    def outbox_relayed(self, count: int) -> None:
        for part in self.parts:
            part.outbox_relayed(count)

    def worker_error(self, loop: str) -> None:
        for part in self.parts:
            part.worker_error(loop)

    def history_size_warning(self, run_id: str, workflow_name: str, size: int, unit: str) -> None:
        for part in self.parts:
            part.history_size_warning(run_id, workflow_name, size, unit)


class MetricsTelemetry(NoopTelemetry):
    """Records Loomwork's standard metrics in a :class:`MetricsRegistry`."""

    def __init__(self, registry: MetricsRegistry | None = None) -> None:
        self.registry = registry or MetricsRegistry()
        r = self.registry
        self.ticks = r.counter(
            "loomwork_ticks_total", "Workflow ticks processed.", ["workflow", "outcome"]
        )
        self.tick_seconds = r.histogram(
            "loomwork_tick_duration_seconds", "Time to process one workflow tick.", ["workflow"]
        )
        self.replayed = r.counter(
            "loomwork_replayed_ticks_total",
            "Ticks re-executed from history (cache misses make this grow).",
            ["workflow"],
        )
        self.cache = r.counter("loomwork_replay_cache_total", "Replay cache lookups.", ["result"])
        self.attempts = r.counter(
            "loomwork_step_attempts_total",
            "Step attempts by outcome.",
            ["step", "queue", "outcome"],
        )
        self.attempt_seconds = r.histogram(
            "loomwork_step_attempt_duration_seconds",
            "Duration of step attempts.",
            ["step", "queue"],
        )
        self.queue_delay = r.histogram(
            "loomwork_step_queue_delay_seconds",
            "Delay between a step job becoming ready and its attempt starting.",
            ["queue"],
        )
        self.running = r.gauge(
            "loomwork_step_attempts_running", "Step attempts currently executing.", ["queue"]
        )
        self.schedule_runs = r.counter(
            "loomwork_schedule_runs_total", "Runs started or skipped by schedules.", ["result"]
        )
        self.outbox = r.counter(
            "loomwork_outbox_messages_total", "Cross-run messages relayed from the outbox."
        )
        self.errors = r.counter(
            "loomwork_worker_errors_total", "Unexpected errors in worker loops.", ["loop"]
        )
        self.history_warnings = r.counter(
            "loomwork_history_size_warnings_total",
            "Runs whose history crossed the configured warning threshold.",
            ["workflow", "unit"],
        )

    def tick_finished(self, report: TickReport) -> None:
        self.ticks.inc(workflow=report.workflow_name, outcome=report.outcome)
        self.tick_seconds.observe(report.duration, workflow=report.workflow_name)
        if report.replayed_ticks:
            self.replayed.inc(report.replayed_ticks, workflow=report.workflow_name)
        self.cache.inc(result="hit" if report.cache_hit else "miss")

    @contextlib.contextmanager
    def step_attempt(self, info: AttemptInfo) -> Iterator[None]:
        self.queue_delay.observe(max(info.queue_delay, 0.0), queue=info.queue)
        self.running.inc(queue=info.queue)
        try:
            yield
        finally:
            self.running.dec(queue=info.queue)

    def step_attempt_finished(self, report: AttemptReport) -> None:
        info = report.info
        self.attempts.inc(step=info.step_name, queue=info.queue, outcome=report.outcome)
        self.attempt_seconds.observe(report.duration, step=info.step_name, queue=info.queue)

    def schedule_evaluated(self, schedule_id: str, started: int, skipped: int) -> None:
        if started:
            self.schedule_runs.inc(started, result="started")
        if skipped:
            self.schedule_runs.inc(skipped, result="skipped")

    def outbox_relayed(self, count: int) -> None:
        if count:
            self.outbox.inc(count)

    def worker_error(self, loop: str) -> None:
        self.errors.inc(loop=loop)

    def history_size_warning(self, run_id: str, workflow_name: str, size: int, unit: str) -> None:
        self.history_warnings.inc(workflow=workflow_name, unit=unit)


class LoggingTelemetry(NoopTelemetry):
    """Emits one structured log record per tick and per finished step attempt.

    Records go to the ``loomwork.telemetry`` logger at DEBUG level (failures at INFO), with
    the fields in ``extra`` so JSON log formatters pick them up.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger("loomwork.telemetry")

    def tick_finished(self, report: TickReport) -> None:
        level = logging.INFO if report.outcome == "failed" else logging.DEBUG
        self.logger.log(
            level,
            "tick %s for %s in %.1fms",
            report.outcome,
            report.workflow_name,
            report.duration * 1000,
            extra={
                "run_id": report.run_id,
                "workflow_name": report.workflow_name,
                "outcome": report.outcome,
                "duration_ms": round(report.duration * 1000, 3),
                "commands": report.commands,
                "cache_hit": report.cache_hit,
            },
        )

    def step_attempt_finished(self, report: AttemptReport) -> None:
        info = report.info
        level = logging.INFO if report.outcome in {"failed", "retrying"} else logging.DEBUG
        self.logger.log(
            level,
            "step %s attempt %d %s in %.1fms",
            info.step_name,
            info.attempt,
            report.outcome,
            report.duration * 1000,
            extra={
                "run_id": info.run_id,
                "step_name": info.step_name,
                "attempt": info.attempt,
                "queue": info.queue,
                "outcome": report.outcome,
                "failure_type": report.failure_type,
                "duration_ms": round(report.duration * 1000, 3),
            },
        )

    def history_size_warning(self, run_id: str, workflow_name: str, size: int, unit: str) -> None:
        self.logger.warning(
            "run %s of %s has a history of %d %s and may need continue-as-new",
            run_id,
            workflow_name,
            size,
            unit,
            extra={
                "run_id": run_id,
                "workflow_name": workflow_name,
                "history_size": size,
                "history_size_unit": unit,
            },
        )
