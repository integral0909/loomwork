"""Shared diagnostics: the facts ``loomwork doctor`` and the server's control-API
diagnostics endpoint both report, and the pass/warn/fail findings built from them.

Computed once, here, against a :class:`~loomwork.store.base.Store`/
:class:`~loomwork.engine.engine.Engine` pair, so the CLI's local (``--store``) path and
the server's ``GET /control/diagnostics`` endpoint -- which the CLI's ``--server`` path
calls instead, since it has no direct store access -- can never disagree about what
"unhealthy" means; both go through :func:`collect_diagnostics`, and
:class:`DiagnosticsSnapshot` is the JSON-serializable shape that travels between them.
:func:`build_findings` then turns a snapshot into the same report text everywhere it is
shown: ``loomwork doctor``'s own output and the console's diagnostics page both call it
directly, rather than each formatting the snapshot's raw numbers their own way.

Every query this module makes is bounded by ``limit`` (and, where the store gives no way
to enumerate the underlying set at all -- task queues -- by an explicit ``queues``
argument), so running it against a large production store is safe.
"""

from __future__ import annotations

import enum
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from loomwork.engine.engine import Engine
from loomwork.store.base import Store
from loomwork.store.records import ExecutionStatus

#: Defaults shared by the CLI's flags and the control API's query parameters, so a bare
#: ``loomwork doctor`` and a bare ``GET /control/diagnostics`` agree.
DEFAULT_TICK_FAILURE_THRESHOLD = 3
DEFAULT_HISTORY_WARNING_FRACTION = 0.9
DEFAULT_LIMIT = 100


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_iso(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


@dataclass(frozen=True, slots=True)
class TickFailure:
    """A running run whose ticks keep failing."""

    run_id: str
    workflow_id: str
    workflow_name: str
    tick_failures: int
    last_tick_error: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "tick_failures": self.tick_failures,
            "last_tick_error": self.last_tick_error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TickFailure:
        return cls(
            run_id=str(data["run_id"]),
            workflow_id=str(data["workflow_id"]),
            workflow_name=str(data["workflow_name"]),
            tick_failures=int(data["tick_failures"]),
            last_tick_error=data.get("last_tick_error"),
        )


@dataclass(frozen=True, slots=True)
class OverdueSchedule:
    """A schedule whose next fire time is already in the past."""

    schedule_id: str
    next_fire_at: datetime
    overdue_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "schedule_id": self.schedule_id,
            "next_fire_at": self.next_fire_at.isoformat(),
            "overdue_seconds": self.overdue_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OverdueSchedule:
        return cls(
            schedule_id=str(data["schedule_id"]),
            next_fire_at=datetime.fromisoformat(data["next_fire_at"]),
            overdue_seconds=float(data["overdue_seconds"]),
        )


@dataclass(frozen=True, slots=True)
class LargeHistory:
    """A running run approaching :attr:`~loomwork.engine.config.EngineConfig
    .history_limit_events` or ``history_limit_bytes``."""

    run_id: str
    workflow_id: str
    workflow_name: str
    last_event_id: int
    history_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "last_event_id": self.last_event_id,
            "history_bytes": self.history_bytes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LargeHistory:
        return cls(
            run_id=str(data["run_id"]),
            workflow_id=str(data["workflow_id"]),
            workflow_name=str(data["workflow_name"]),
            last_event_id=int(data["last_event_id"]),
            history_bytes=int(data["history_bytes"]),
        )


@dataclass(frozen=True, slots=True)
class QueueBacklog:
    """The oldest unclaimed, ready-to-run step job on one queue.

    ``queue=None`` means "every queue" (the store has no way to enumerate the distinct
    queues in use, so a per-queue check needs the queue name up front); it is always the
    first entry of :attr:`DiagnosticsSnapshot.queue_backlogs`.
    """

    queue: str | None
    oldest_ready_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {"queue": self.queue, "oldest_ready_at": _iso(self.oldest_ready_at)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueueBacklog:
        return cls(queue=data.get("queue"), oldest_ready_at=_parse_iso(data.get("oldest_ready_at")))


@dataclass(frozen=True, slots=True)
class DiagnosticsSnapshot:
    """Everything :func:`collect_diagnostics` found, as of ``now``."""

    now: datetime
    schema_version: int | None
    latest_known_schema_version: int | None
    outbox_backlog: int
    running_runs: int
    running_runs_capped: bool
    failed_runs: int
    failed_runs_capped: bool
    tick_failure_threshold: int
    tick_failing: tuple[TickFailure, ...]
    tick_failing_capped: bool
    next_timer_at: datetime | None
    queue_backlogs: tuple[QueueBacklog, ...]
    schedules_overdue: tuple[OverdueSchedule, ...]
    schedules_overdue_capped: bool
    large_histories: tuple[LargeHistory, ...]
    large_histories_capped: bool
    history_limit_bytes: int | None
    history_limit_events: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "now": self.now.isoformat(),
            "schema_version": self.schema_version,
            "latest_known_schema_version": self.latest_known_schema_version,
            "outbox_backlog": self.outbox_backlog,
            "running_runs": self.running_runs,
            "running_runs_capped": self.running_runs_capped,
            "failed_runs": self.failed_runs,
            "failed_runs_capped": self.failed_runs_capped,
            "tick_failure_threshold": self.tick_failure_threshold,
            "tick_failing": [t.to_dict() for t in self.tick_failing],
            "tick_failing_capped": self.tick_failing_capped,
            "next_timer_at": _iso(self.next_timer_at),
            "queue_backlogs": [q.to_dict() for q in self.queue_backlogs],
            "schedules_overdue": [s.to_dict() for s in self.schedules_overdue],
            "schedules_overdue_capped": self.schedules_overdue_capped,
            "large_histories": [h.to_dict() for h in self.large_histories],
            "large_histories_capped": self.large_histories_capped,
            "history_limit_bytes": self.history_limit_bytes,
            "history_limit_events": self.history_limit_events,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DiagnosticsSnapshot:
        return cls(
            now=datetime.fromisoformat(data["now"]),
            schema_version=data.get("schema_version"),
            latest_known_schema_version=data.get("latest_known_schema_version"),
            outbox_backlog=int(data["outbox_backlog"]),
            running_runs=int(data["running_runs"]),
            running_runs_capped=bool(data["running_runs_capped"]),
            failed_runs=int(data["failed_runs"]),
            failed_runs_capped=bool(data["failed_runs_capped"]),
            tick_failure_threshold=int(data["tick_failure_threshold"]),
            tick_failing=tuple(TickFailure.from_dict(t) for t in data.get("tick_failing", [])),
            tick_failing_capped=bool(data["tick_failing_capped"]),
            next_timer_at=_parse_iso(data.get("next_timer_at")),
            queue_backlogs=tuple(QueueBacklog.from_dict(q) for q in data.get("queue_backlogs", [])),
            schedules_overdue=tuple(
                OverdueSchedule.from_dict(s) for s in data.get("schedules_overdue", [])
            ),
            schedules_overdue_capped=bool(data["schedules_overdue_capped"]),
            large_histories=tuple(
                LargeHistory.from_dict(h) for h in data.get("large_histories", [])
            ),
            large_histories_capped=bool(data["large_histories_capped"]),
            history_limit_bytes=data.get("history_limit_bytes"),
            history_limit_events=int(data["history_limit_events"]),
        )


async def collect_diagnostics(
    store: Store,
    engine: Engine,
    now: datetime,
    *,
    tick_failure_threshold: int = DEFAULT_TICK_FAILURE_THRESHOLD,
    history_warning_fraction: float = DEFAULT_HISTORY_WARNING_FRACTION,
    queues: Sequence[str] = (),
    limit: int = DEFAULT_LIMIT,
) -> DiagnosticsSnapshot:
    """Collect one point-in-time diagnostics snapshot.

    Args:
        store: the store to inspect.
        engine: the engine on top of it (for its configuration and visibility queries).
        now: the moment "overdue" and "stale" are measured against; the caller's own
            clock, so a store bound to a :class:`~loomwork.clock.ManualClock` in tests
            gets consistent results.
        tick_failure_threshold: a running run with at least this many consecutive tick
            failures is reported.
        history_warning_fraction: a run whose history has reached this fraction of
            :attr:`~loomwork.engine.config.EngineConfig.history_limit_events` (or
            ``history_limit_bytes``, when configured) is reported.
        queues: task queue names to check individually for a stale backlog, in addition
            to the store-wide check that always runs; the store has no way to enumerate
            queues in use, so a per-queue check needs to be told which ones matter.
        limit: caps every query this function makes.
    """
    schema_version_fn = getattr(store, "schema_version", None)
    schema_version = await schema_version_fn() if schema_version_fn is not None else None
    latest_known_schema_version = None
    if schema_version_fn is not None:
        from loomwork.store.sql.migrations import MIGRATIONS

        latest_known_schema_version = max(m.version for m in MIGRATIONS)

    outbox_backlog = await store.outbox_size()

    running = await store.list_executions(status=ExecutionStatus.RUNNING, limit=limit)
    failed = await store.list_executions(status=ExecutionStatus.FAILED, limit=limit)

    tick_failing_records = await engine.query_executions(
        f"status = 'running' AND tick_failures >= {int(tick_failure_threshold)} "
        "ORDER BY tick_failures DESC",
        limit,
    )
    tick_failing = tuple(
        TickFailure(r.run_id, r.workflow_id, r.workflow_name, r.tick_failures, r.last_tick_error)
        for r in tick_failing_records
    )

    next_timer_at = await store.next_timer_at()

    queue_backlogs = [QueueBacklog(None, await store.next_step_ready_at())] + [
        QueueBacklog(queue, await store.next_step_ready_at(queues=[queue])) for queue in queues
    ]

    schedules = await store.list_schedules(limit)
    overdue = [
        OverdueSchedule(s.schedule_id, s.next_fire_at, (now - s.next_fire_at).total_seconds())
        for s in schedules
        if not s.paused and s.next_fire_at is not None and s.next_fire_at < now
    ]

    history_limit_bytes = engine.config.history_limit_bytes
    history_limit_events = engine.config.history_limit_events
    large_histories = [
        LargeHistory(r.run_id, r.workflow_id, r.workflow_name, r.last_event_id, r.history_bytes)
        for r in running
        if r.last_event_id >= history_limit_events * history_warning_fraction
        or (
            history_limit_bytes is not None
            and r.history_bytes >= history_limit_bytes * history_warning_fraction
        )
    ]

    return DiagnosticsSnapshot(
        now=now,
        schema_version=schema_version,
        latest_known_schema_version=latest_known_schema_version,
        outbox_backlog=outbox_backlog,
        running_runs=len(running),
        running_runs_capped=len(running) >= limit,
        failed_runs=len(failed),
        failed_runs_capped=len(failed) >= limit,
        tick_failure_threshold=tick_failure_threshold,
        tick_failing=tick_failing,
        tick_failing_capped=len(tick_failing) >= limit,
        next_timer_at=next_timer_at,
        queue_backlogs=tuple(queue_backlogs),
        schedules_overdue=tuple(overdue[:limit]),
        schedules_overdue_capped=len(overdue) > limit,
        large_histories=tuple(large_histories[:limit]),
        large_histories_capped=len(large_histories) > limit,
        history_limit_bytes=history_limit_bytes,
        history_limit_events=history_limit_events,
    )


#: Defaults for the thresholds :func:`build_findings` itself applies (as opposed to the
#: ones above, which shape what :func:`collect_diagnostics` even looks at).
DEFAULT_OUTBOX_BACKLOG_THRESHOLD = 100
DEFAULT_TIMER_OVERDUE_SECONDS = 300.0
DEFAULT_STEP_STALL_SECONDS = 300.0
DEFAULT_SCHEDULE_OVERDUE_SECONDS = 300.0
#: Clock skew (seconds) between this machine and a server above which the check warns,
#: and above which it fails.
CLOCK_SKEW_WARN_SECONDS = 2.0
CLOCK_SKEW_FAIL_SECONDS = 10.0


class Status(enum.StrEnum):
    """One finding's severity. Only :attr:`FAIL` makes ``loomwork doctor`` exit 1."""

    PASS = "pass"  # noqa: S105 - a severity level, not a credential
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class Finding:
    """One line of a diagnostics report."""

    check: str
    status: Status
    message: str
    hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "status": self.status.value,
            "message": self.message,
            "hint": self.hint,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Finding:
        return cls(
            check=str(data["check"]),
            status=Status(data["status"]),
            message=str(data["message"]),
            hint=data.get("hint"),
        )


def _sample(items: Sequence[str], limit: int = 5) -> str:
    shown = list(items[:limit])
    if len(items) > limit:
        shown.append(f"... and {len(items) - limit} more")
    return ", ".join(shown)


def _schema_finding(snapshot: DiagnosticsSnapshot) -> Finding:
    version, latest = snapshot.schema_version, snapshot.latest_known_schema_version
    if version is None:
        return Finding("schema", Status.PASS, "no schema to migrate (in-memory store)")
    if latest is None:
        return Finding("schema", Status.PASS, f"schema at version {version}")
    if version < latest:
        return Finding(
            "schema",
            Status.WARN,
            f"schema is at version {version}; {latest} is the newest this release knows",
            "run `loomwork store migrate`",
        )
    if version > latest:
        return Finding(
            "schema",
            Status.FAIL,
            f"schema is at version {version}, newer than the {latest} this Loomwork "
            "release knows",
            "upgrade this machine's Loomwork installation",
        )
    return Finding("schema", Status.PASS, f"schema at version {version}, up to date")


def _clock_skew_finding(skew_seconds: float) -> Finding:
    skew = abs(skew_seconds)
    if skew >= CLOCK_SKEW_FAIL_SECONDS:
        return Finding(
            "clock skew",
            Status.FAIL,
            f"{skew:.1f}s between this machine and the server",
            "synchronize clocks (NTP); large skew causes premature timeouts and "
            "confusing timestamps",
        )
    if skew >= CLOCK_SKEW_WARN_SECONDS:
        return Finding(
            "clock skew",
            Status.WARN,
            f"{skew:.1f}s between this machine and the server",
            "synchronize clocks (NTP)",
        )
    return Finding("clock skew", Status.PASS, f"{skew:.1f}s")


def _outbox_finding(snapshot: DiagnosticsSnapshot, threshold: int) -> Finding:
    if snapshot.outbox_backlog == 0:
        return Finding("outbox backlog", Status.PASS, "empty")
    status = Status.FAIL if snapshot.outbox_backlog >= threshold else Status.WARN
    return Finding(
        "outbox backlog",
        status,
        f"{snapshot.outbox_backlog} message(s) waiting (threshold {threshold})",
        "check that a worker is running with run_outbox=True",
    )


def _tick_failures_finding(snapshot: DiagnosticsSnapshot) -> Finding:
    threshold = snapshot.tick_failure_threshold
    if not snapshot.tick_failing:
        return Finding(
            "tick failures",
            Status.PASS,
            f"no running run has >= {threshold} consecutive" " tick failures",
        )
    sample = _sample(
        [
            f"{t.workflow_id} ({t.tick_failures}x: {t.last_tick_error})"
            for t in snapshot.tick_failing
        ]
    )
    count = f"{len(snapshot.tick_failing)}{'+' if snapshot.tick_failing_capped else ''}"
    return Finding(
        "tick failures",
        Status.FAIL,
        f"{count} run(s) with >= {threshold} consecutive tick failures: {sample}",
        "see `loomwork workflow describe`/`history`; a NondeterminismError needs a code "
        "fix or loomwork.patched(), see docs/guides/versioning.md",
    )


def _queue_findings(snapshot: DiagnosticsSnapshot, step_stall_seconds: float) -> list[Finding]:
    findings = []
    for backlog in snapshot.queue_backlogs:
        label = f"step queue {backlog.queue!r}" if backlog.queue else "step job backlog"
        if backlog.oldest_ready_at is None:
            findings.append(Finding(label, Status.PASS, "no ready step jobs waiting"))
            continue
        age = (snapshot.now - backlog.oldest_ready_at).total_seconds()
        if age < step_stall_seconds:
            findings.append(Finding(label, Status.PASS, f"oldest ready job is {age:.0f}s old"))
        else:
            findings.append(
                Finding(
                    label,
                    Status.FAIL,
                    f"oldest ready job is {age:.0f}s old (threshold {step_stall_seconds:.0f}s)",
                    "no live worker is serving this queue: check task_queues, or `--queue`",
                )
            )
    return findings


def _timers_finding(snapshot: DiagnosticsSnapshot, timer_overdue_seconds: float) -> Finding:
    if snapshot.next_timer_at is None:
        return Finding("timers", Status.PASS, "no outstanding timers")
    age = (snapshot.now - snapshot.next_timer_at).total_seconds()
    if age < timer_overdue_seconds:
        return Finding("timers", Status.PASS, "no timer is overdue")
    return Finding(
        "timers",
        Status.FAIL,
        f"a timer is overdue by {age:.0f}s (threshold {timer_overdue_seconds:.0f}s)",
        "start a worker with run_timers=True",
    )


def _schedules_finding(snapshot: DiagnosticsSnapshot, schedule_overdue_seconds: float) -> Finding:
    if not snapshot.schedules_overdue:
        return Finding("schedules", Status.PASS, "no schedule is overdue")
    worst = max(s.overdue_seconds for s in snapshot.schedules_overdue)
    status = Status.FAIL if worst >= schedule_overdue_seconds else Status.WARN
    sample = _sample([s.schedule_id for s in snapshot.schedules_overdue])
    capped = "+" if snapshot.schedules_overdue_capped else ""
    return Finding(
        "schedules",
        status,
        f"{len(snapshot.schedules_overdue)}{capped} schedule(s) overdue, worst by "
        f"{worst:.0f}s: {sample}",
        "start a worker with run_schedules=True",
    )


def _history_size_finding(snapshot: DiagnosticsSnapshot) -> Finding:
    if not snapshot.large_histories:
        return Finding("history size", Status.PASS, "no run is near its history limit")
    sample = _sample([h.workflow_id for h in snapshot.large_histories])
    capped = "+" if snapshot.large_histories_capped else ""
    return Finding(
        "history size",
        Status.WARN,
        f"{len(snapshot.large_histories)}{capped} run(s) near the history limit: {sample}",
        "use loomwork.continue_as_new_suggested(), or raise history_limit_events/bytes",
    )


def _token_finding(roles: Sequence[str], namespaces: Sequence[str] | None) -> Finding:
    return Finding(
        "token",
        Status.PASS,
        f"roles: {', '.join(roles) or 'none'}; namespaces: {', '.join(namespaces or []) or 'none'}",
    )


def build_findings(
    snapshot: DiagnosticsSnapshot,
    *,
    outbox_backlog_threshold: int = DEFAULT_OUTBOX_BACKLOG_THRESHOLD,
    timer_overdue_seconds: float = DEFAULT_TIMER_OVERDUE_SECONDS,
    step_stall_seconds: float = DEFAULT_STEP_STALL_SECONDS,
    schedule_overdue_seconds: float = DEFAULT_SCHEDULE_OVERDUE_SECONDS,
    roles: Sequence[str] | None = None,
    namespaces: Sequence[str] | None = None,
    clock_skew_seconds: float | None = None,
) -> list[Finding]:
    """Turn one snapshot into the report ``loomwork doctor`` prints and the console's
    diagnostics page renders -- the same list of findings either way.

    ``roles``/``namespaces``/``clock_skew_seconds`` are only meaningful in server mode
    (a local ``--store`` connection has no token and shares this machine's clock), so they
    are ``None`` and skipped there.
    """
    findings = [
        _schema_finding(snapshot),
        *([_clock_skew_finding(clock_skew_seconds)] if clock_skew_seconds is not None else []),
        _outbox_finding(snapshot, outbox_backlog_threshold),
        _tick_failures_finding(snapshot),
        *_queue_findings(snapshot, step_stall_seconds),
        _timers_finding(snapshot, timer_overdue_seconds),
        _schedules_finding(snapshot, schedule_overdue_seconds),
        _history_size_finding(snapshot),
    ]
    if roles is not None:
        findings.append(_token_finding(roles, namespaces))
    return findings


__all__ = [
    "CLOCK_SKEW_FAIL_SECONDS",
    "CLOCK_SKEW_WARN_SECONDS",
    "DEFAULT_HISTORY_WARNING_FRACTION",
    "DEFAULT_LIMIT",
    "DEFAULT_OUTBOX_BACKLOG_THRESHOLD",
    "DEFAULT_SCHEDULE_OVERDUE_SECONDS",
    "DEFAULT_STEP_STALL_SECONDS",
    "DEFAULT_TICK_FAILURE_THRESHOLD",
    "DEFAULT_TIMER_OVERDUE_SECONDS",
    "DiagnosticsSnapshot",
    "Finding",
    "LargeHistory",
    "OverdueSchedule",
    "QueueBacklog",
    "Status",
    "TickFailure",
    "build_findings",
    "collect_diagnostics",
]
