"""``loomwork builds ...`` against a real SQLite store: drain status, live workers, and
declaring (and revoking) compatibility -- constructed for real with the same store
primitives a worker and an operator use, not mocked.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loomwork.engine.engine import Engine
from loomwork.options import VersioningPolicy
from loomwork.store.records import TickOutcome
from loomwork.store.sqlite import SQLiteStore
from tests.cli.conftest import Cli


def _run(coro: object) -> None:
    with asyncio.Runner(loop_factory=asyncio.new_event_loop) as runner:
        runner.run(coro)  # type: ignore[arg-type]


async def _pin_a_run(db_path: Path, workflow_id: str, build_id: str) -> None:
    store = SQLiteStore(db_path)
    await store.open()
    try:
        engine = Engine(store)
        record = await engine.start_workflow(
            "builds.noop", [], workflow_id=workflow_id, versioning_policy=VersioningPolicy.PINNED
        )
        now = datetime.now(UTC)
        claimed = await store.claim_tick("w1", now, timedelta(seconds=30), build_id=build_id)
        assert claimed is not None and claimed.run_id == record.run_id
        outcome = TickOutcome(
            worker_id="w1", consumed_through=0, time=now, outbox=[], build_id=build_id
        )
        await store.complete_tick(record.run_id, "w1", outcome, now)
    finally:
        await store.close()


async def _heartbeat(db_path: Path, worker_id: str, build_id: str) -> None:
    store = SQLiteStore(db_path)
    await store.open()
    try:
        now = datetime.now(UTC)
        await store.heartbeat_worker(worker_id, build_id, now, now + timedelta(seconds=60))
    finally:
        await store.close()


def test_list_reports_orphaned_and_live_builds(cli: Cli, db_path: Path) -> None:
    _run(_pin_a_run(db_path, "pinned-1", "build-orphan"))
    _run(_heartbeat(db_path, "worker-1", "build-live"))

    data = {row["build_id"]: row for row in json.loads(cli.ok("builds", "list", "--json").out)}
    assert data["build-orphan"]["pinned_running_runs"] == 1
    assert data["build-orphan"]["live_workers"] == 0
    assert data["build-orphan"]["orphaned"] is True
    assert data["build-live"]["live_workers"] == 1
    assert data["build-live"]["orphaned"] is False

    workers = json.loads(cli.ok("builds", "workers", "--json").out)
    assert {w["worker_id"]: w["build_id"] for w in workers} == {"worker-1": "build-live"}


def test_declare_compatible_drains_an_orphaned_build(cli: Cli, db_path: Path) -> None:
    _run(_pin_a_run(db_path, "pinned-2", "build-old"))
    data = {row["build_id"]: row for row in json.loads(cli.ok("builds", "list", "--json").out)}
    assert data["build-old"]["orphaned"] is True

    _run(_heartbeat(db_path, "worker-2", "build-new"))
    cli.ok("builds", "compatible", "build-new", "--with", "build-old")

    data = {row["build_id"]: row for row in json.loads(cli.ok("builds", "list", "--json").out)}
    assert data["build-old"]["orphaned"] is False
    assert data["build-old"]["compatible_with"] == []
    assert data["build-new"]["compatible_with"] == ["build-old"]

    cli.ok("builds", "compatible", "build-new", "--with", "build-old", "--revoke")
    data = {row["build_id"]: row for row in json.loads(cli.ok("builds", "list", "--json").out)}
    assert data["build-old"]["orphaned"] is True
    assert data["build-new"]["compatible_with"] == []


def test_revoke_of_undeclared_compatibility_is_a_usage_error(cli: Cli) -> None:
    result = cli("builds", "compatible", "build-x", "--with", "build-y", "--revoke")
    assert result.code == 2


def test_compatible_requires_with(cli: Cli) -> None:
    result = cli("builds", "compatible", "build-x")
    assert result.code == 2
