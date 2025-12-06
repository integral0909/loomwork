"""``loomwork doctor``.

Each unhealthy condition is constructed for real against a plain :class:`SQLiteStore`
using the same store/engine/worker primitives production code uses (a claimed tick that
is then failed, a step job on a queue no worker serves, a timer with no timer-running
worker, a schedule with no schedule-running worker, outbox messages with no relay worker)
rather than mocked -- ``loomwork doctor`` is meant to catch exactly these, for real.

A few checks (schema version drift, history size, clock skew) are impractical to trigger
through a real store in a fast test (drift needs two different installed versions; the
history limit defaults to 50,000 events; clock skew needs a second machine), so those are
covered as direct unit tests of :func:`~loomwork.diagnostics.build_findings` against a
hand-built snapshot instead.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import loomwork as lw
from loomwork.clock import SystemClock
from loomwork.diagnostics import (
    DiagnosticsSnapshot,
    Finding,
    OverdueSchedule,
    QueueBacklog,
    Status,
    TickFailure,
    build_findings,
)
from loomwork.engine.engine import Engine
from loomwork.schedules.model import ScheduleAction, SchedulePolicy, ScheduleSpec, ScheduleState
from loomwork.schedules.service import ScheduleService
from loomwork.sdk.definitions import Registry
from loomwork.store.records import OutboxAction, OutboxMessage, TickOutcome
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker.worker import Worker, WorkerOptions
from tests.cli.conftest import Cli


def _run(coro_factory: object) -> None:
    with asyncio.Runner(loop_factory=asyncio.new_event_loop) as runner:
        runner.run(coro_factory)  # type: ignore[arg-type]


async def _open(path: Path) -> SQLiteStore:
    store = SQLiteStore(path)
    await store.open()
    return store


def _finding(result_out: str, check: str) -> str:
    """The one report line for ``check`` (there is exactly one per check)."""
    lines = [line for line in result_out.splitlines() if f"] {check} " in line + " "]
    assert len(lines) == 1, f"{check!r} not found in:\n{result_out}"
    return lines[0]


# --------------------------------------------------------------------------- healthy


def test_healthy_store_passes_every_check(cli: Cli) -> None:
    result = cli.ok("doctor")
    assert result.code == 0
    assert "FAIL" not in result.out
    assert "all checks passed" in result.err


def test_json_output_shape(cli: Cli) -> None:
    result = cli.ok("doctor", "--json")
    data = json.loads(result.out)
    assert data["ok"] is True
    assert all(f["status"] == "pass" for f in data["findings"])
    assert {f["check"] for f in data["findings"]} >= {
        "schema",
        "outbox backlog",
        "tick failures",
        "timers",
        "schedules",
        "history size",
    }


# --------------------------------------------------------------------------- tick failures


def test_tick_failures_are_reported(cli: Cli, db_path: Path) -> None:
    async def setup() -> None:
        store = await _open(db_path)
        try:
            engine = Engine(store)
            record = await engine.start_workflow("doctor.stuck", [], workflow_id="stuck-1")
            now = datetime.now(UTC)
            claimed = await store.claim_tick("w1", now, timedelta(seconds=30))
            assert claimed is not None and claimed.run_id == record.run_id
            await store.fail_tick(record.run_id, "w1", "NondeterminismError: boom", now, now)
        finally:
            await store.close()

    _run(setup())

    result = cli("doctor", "--tick-failure-threshold", "1")
    assert result.code == 1
    line = _finding(result.out, "tick failures")
    assert line.startswith("[FAIL]")
    assert "stuck-1" in line
    assert "NondeterminismError" in line


# --------------------------------------------------------------------------- step queues


@lw.step(name="doctor.reserved_step", queue="reserved")
async def _reserved_step() -> None:
    return None


@lw.workflow(name="doctor.queued")
async def _queued_workflow() -> None:
    await _reserved_step()


_QUEUE_REGISTRY = Registry(workflows=[_queued_workflow], steps=[_reserved_step])


def test_stale_step_queue_is_reported(cli: Cli, db_path: Path) -> None:
    async def setup() -> None:
        store = await _open(db_path)
        try:
            # A worker that serves every queue *except* "reserved": the tick still runs
            # (workflow ticks are not queue-scoped) and schedules the step, but nothing
            # ever claims it.
            worker = Worker(
                store,
                _QUEUE_REGISTRY,
                options=WorkerOptions(
                    task_queues=("other",),
                    run_timers=False,
                    run_outbox=False,
                    run_schedules=False,
                ),
            )
            async with worker:
                engine = Engine(store)
                record = await engine.start_workflow("doctor.queued", [], workflow_id="queued-1")
                deadline = asyncio.get_running_loop().time() + 5.0
                while not await store.get_step_jobs(record.run_id):
                    if asyncio.get_running_loop().time() > deadline:
                        raise AssertionError("step job never appeared")
                    await asyncio.sleep(0.02)
        finally:
            await store.close()

    _run(setup())

    result = cli("doctor", "--queue", "reserved", "--step-stall", "0s")
    assert result.code == 1
    line = _finding(result.out, "step queue 'reserved'")
    assert line.startswith("[FAIL]")
    assert "no live worker" in result.out


def test_step_queue_not_requested_is_not_checked_individually(cli: Cli, db_path: Path) -> None:
    async def setup() -> None:
        store = await _open(db_path)
        try:
            worker = Worker(
                store,
                _QUEUE_REGISTRY,
                options=WorkerOptions(
                    task_queues=("other",),
                    run_timers=False,
                    run_outbox=False,
                    run_schedules=False,
                ),
            )
            async with worker:
                engine = Engine(store)
                record = await engine.start_workflow("doctor.queued", [], workflow_id="queued-2")
                deadline = asyncio.get_running_loop().time() + 5.0
                while not await store.get_step_jobs(record.run_id):
                    if asyncio.get_running_loop().time() > deadline:
                        raise AssertionError("step job never appeared")
                    await asyncio.sleep(0.02)
        finally:
            await store.close()

    _run(setup())

    # Without --queue, only the store-wide backlog is checked (which also sees the stale
    # job, since it aggregates every queue) -- there is no per-queue "reserved" line.
    result = cli("doctor", "--step-stall", "0s")
    assert result.code == 1
    assert "step job backlog" in result.out
    assert "step queue 'reserved'" not in result.out


# --------------------------------------------------------------------------- timers


@lw.workflow(name="doctor.sleeper")
async def _sleeper_workflow() -> None:
    await lw.sleep(0.05)


_TIMER_REGISTRY = Registry(workflows=[_sleeper_workflow])


def test_overdue_timer_is_reported(cli: Cli, db_path: Path) -> None:
    async def setup() -> None:
        store = await _open(db_path)
        try:
            worker = Worker(
                store,
                _TIMER_REGISTRY,
                options=WorkerOptions(run_timers=False, run_outbox=False, run_schedules=False),
            )
            async with worker:
                engine = Engine(store)
                await engine.start_workflow("doctor.sleeper", [], workflow_id="sleeper-1")
                deadline = asyncio.get_running_loop().time() + 5.0
                while await store.next_timer_at() is None:
                    if asyncio.get_running_loop().time() > deadline:
                        raise AssertionError("timer never appeared")
                    await asyncio.sleep(0.02)
                # Let the timer's own fire_at (start + 0.05s) fall behind real time.
                await asyncio.sleep(0.1)
        finally:
            await store.close()

    _run(setup())

    result = cli("doctor", "--timer-overdue", "0s")
    assert result.code == 1
    line = _finding(result.out, "timers")
    assert line.startswith("[FAIL]")
    assert "run_timers=True" in result.out


# --------------------------------------------------------------------------- schedules


def test_overdue_schedule_is_reported(cli: Cli, db_path: Path) -> None:
    async def setup() -> None:
        store = await _open(db_path)
        try:
            service = ScheduleService(store, SystemClock())
            state = ScheduleState(
                schedule_id="doctor-sched",
                spec=ScheduleSpec(interval=1),
                action=ScheduleAction("doctor.unused"),
                policy=SchedulePolicy(),
            )
            await service.create(state)
        finally:
            await store.close()

    _run(setup())
    # The schedule's next fire time is at most 1s after creation (a 1s interval); nothing
    # ever evaluates it (no worker with run_schedules=True), so it falls further behind.
    time.sleep(1.2)

    result = cli("doctor", "--schedule-overdue", "0s")
    assert result.code == 1
    line = _finding(result.out, "schedules")
    assert line.startswith("[FAIL]")
    assert "doctor-sched" in line
    assert "run_schedules=True" in result.out


# --------------------------------------------------------------------------- outbox


def test_outbox_backlog_is_reported(cli: Cli, db_path: Path) -> None:
    async def setup() -> None:
        store = await _open(db_path)
        try:
            engine = Engine(store)
            record = await engine.start_workflow("doctor.noop", [], workflow_id="noop-1")
            now = datetime.now(UTC)
            claimed = await store.claim_tick("w1", now, timedelta(seconds=30))
            assert claimed is not None
            outcome = TickOutcome(
                worker_id="w1",
                consumed_through=0,
                time=now,
                outbox=[
                    OutboxMessage(OutboxAction.TERMINATE, target_run_id=f"ghost-{i}")
                    for i in range(5)
                ],
            )
            await store.complete_tick(record.run_id, "w1", outcome, now)
        finally:
            await store.close()

    _run(setup())

    result = cli("doctor", "--outbox-backlog-threshold", "3")
    assert result.code == 1
    line = _finding(result.out, "outbox backlog")
    assert line.startswith("[FAIL]")
    assert "run_outbox=True" in result.out


# --------------------------------------------------------------------------- build_findings unit tests


def _snapshot(**overrides: object) -> DiagnosticsSnapshot:
    base: dict[str, object] = {
        "now": datetime(2024, 1, 1, tzinfo=UTC),
        "schema_version": 5,
        "latest_known_schema_version": 5,
        "outbox_backlog": 0,
        "running_runs": 0,
        "running_runs_capped": False,
        "failed_runs": 0,
        "failed_runs_capped": False,
        "tick_failure_threshold": 3,
        "tick_failing": (),
        "tick_failing_capped": False,
        "next_timer_at": None,
        "queue_backlogs": (QueueBacklog(None, None),),
        "schedules_overdue": (),
        "schedules_overdue_capped": False,
        "large_histories": (),
        "large_histories_capped": False,
        "history_limit_bytes": None,
        "history_limit_events": 50_000,
    }
    base.update(overrides)
    return DiagnosticsSnapshot(**base)  # type: ignore[arg-type]


def _find(findings: list[Finding], check: str) -> Finding:
    return next(f for f in findings if f.check == check)


def test_schema_behind_warns() -> None:
    findings = build_findings(_snapshot(schema_version=3, latest_known_schema_version=5))
    finding = _find(findings, "schema")
    assert finding.status is Status.WARN
    assert "loomwork store migrate" in (finding.hint or "")


def test_schema_ahead_fails() -> None:
    findings = build_findings(_snapshot(schema_version=6, latest_known_schema_version=5))
    finding = _find(findings, "schema")
    assert finding.status is Status.FAIL


def test_history_size_warns() -> None:
    from loomwork.diagnostics import LargeHistory

    findings = build_findings(
        _snapshot(large_histories=(LargeHistory("r1", "wf-1", "big.wf", 49_000, 0),))
    )
    finding = _find(findings, "history size")
    assert finding.status is Status.WARN
    assert "wf-1" in finding.message


def test_clock_skew_pass_warn_fail() -> None:
    assert _find(build_findings(_snapshot(), clock_skew_seconds=0.1), "clock skew").status is (
        Status.PASS
    )
    assert _find(build_findings(_snapshot(), clock_skew_seconds=5.0), "clock skew").status is (
        Status.WARN
    )
    assert _find(build_findings(_snapshot(), clock_skew_seconds=30.0), "clock skew").status is (
        Status.FAIL
    )


def test_token_finding_only_present_when_roles_given() -> None:
    assert build_findings(_snapshot()) == [
        f for f in build_findings(_snapshot()) if f.check != "token"
    ]
    with_roles = build_findings(_snapshot(), roles=["reader"], namespaces=["default"])
    finding = _find(with_roles, "token")
    assert "reader" in finding.message
    assert "default" in finding.message


def test_tick_failing_capped_shows_plus() -> None:
    failing = tuple(TickFailure(f"r{i}", f"wf-{i}", "x", 9, "boom") for i in range(3))
    findings = build_findings(_snapshot(tick_failing=failing, tick_failing_capped=True))
    finding = _find(findings, "tick failures")
    assert finding.status is Status.FAIL
    assert "3+" in finding.message


def test_schedules_overdue_below_threshold_warns_not_fails() -> None:
    overdue = (OverdueSchedule("s1", datetime(2024, 1, 1, tzinfo=UTC), overdue_seconds=1.0),)
    findings = build_findings(_snapshot(schedules_overdue=overdue), schedule_overdue_seconds=300.0)
    finding = _find(findings, "schedules")
    assert finding.status is Status.WARN
