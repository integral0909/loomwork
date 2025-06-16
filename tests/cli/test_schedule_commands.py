"""``loomwork schedule ...``."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from tests.cli.conftest import Cli, finish


def create(cli: Cli, schedule_id: str = "nightly", *extra: str) -> dict[str, object]:
    result = cli.ok(
        "schedule",
        "create",
        schedule_id,
        "cli.greet",
        '"cron"',
        "--cron",
        "0 3 * * *",
        *extra,
        "--json",
    )
    data: dict[str, object] = json.loads(result.out)
    return data


def test_create_list_and_describe(cli: Cli) -> None:
    created = create(cli, "nightly", "--tz", "Europe/Stockholm", "--note", "reports")
    assert created["schedule_id"] == "nightly"
    assert created["next_fire_at"]

    listed = cli.ok("schedule", "list")
    header, row = listed.lines
    assert header.split()[:2] == ["SCHEDULE", "WORKFLOW"]
    assert row.split()[0] == "nightly"
    assert "cron '0 3 * * *' (Europe/Stockholm)" in row
    assert "active" in row

    described = cli.ok("schedule", "describe", "nightly")
    assert "arguments:" in described.out
    assert '["cron"]' in described.out
    assert "note:" in described.out
    assert "overlap:" in described.out


def test_create_needs_exactly_one_of_cron_and_every(cli: Cli) -> None:
    neither = cli("schedule", "create", "s", "cli.greet")
    assert neither.code == 2
    both = cli("schedule", "create", "s", "cli.greet", "--cron", "* * * * *", "--every", "1m")
    assert both.code == 2


def test_invalid_cron_is_a_usage_error(cli: Cli) -> None:
    result = cli("schedule", "create", "bad", "cli.greet", "--cron", "61 * * * *")
    assert result.code == 2
    assert "61" in result.err


def test_duplicate_schedule_fails(cli: Cli) -> None:
    create(cli)
    result = cli("schedule", "create", "nightly", "cli.greet", "--every", "1h")
    assert result.code == 1


def test_interval_schedule(cli: Cli) -> None:
    cli.ok("schedule", "create", "often", "cli.sleeper", "--every", "90m", "--jitter", "30s")
    row = cli.ok("schedule", "ls").lines[1]
    assert "every 1h30m" in row
    upcoming = cli.ok("schedule", "upcoming", "often", "-n", "3", "--json")
    times = [datetime.fromisoformat(t) for t in json.loads(upcoming.out)]
    assert len(times) == 3
    assert (times[1] - times[0]).total_seconds() >= 90 * 60 - 60


def test_pause_unpause(cli: Cli) -> None:
    create(cli)
    paused = cli.ok("schedule", "pause", "nightly", "--note", "maintenance")
    assert "paused" in paused.out
    assert "maintenance" in paused.out
    assert "paused" in cli.ok("schedule", "list").lines[1]
    cli.ok("schedule", "unpause", "nightly")
    assert "active" in cli.ok("schedule", "list").lines[1]


def test_trigger_starts_a_run(cli: Cli, db_path: Path) -> None:
    create(cli)
    triggered = cli.ok("schedule", "trigger", "nightly", "--json")
    run = json.loads(triggered.out)
    assert run["workflow_id"].startswith("nightly")
    finish(db_path, run["workflow_id"])
    assert json.loads(cli.ok("workflow", "result", run["workflow_id"]).out) == "HELLO CRON!"


def test_backfill(cli: Cli) -> None:
    create(cli)
    result = cli.ok(
        "schedule",
        "backfill",
        "nightly",
        "--from",
        "2024-01-01T00:00:00Z",
        "--to",
        "2024-01-04T00:00:00Z",
    )
    assert result.out.strip() == "backfill started 3 run(s)"
    runs = json.loads(cli.ok("workflow", "list", "--workflow", "cli.greet", "--json").out)
    assert len(runs) == 3


def test_backfill_range_must_be_ordered(cli: Cli) -> None:
    create(cli)
    result = cli("schedule", "backfill", "nightly", "--from", "now", "--to", "-1d")
    assert result.code == 2


def test_update_changes_spec_and_policy(cli: Cli) -> None:
    create(cli)
    cli.ok("schedule", "update", "nightly", "--every", "2h", "--overlap", "allow_all")
    data = json.loads(cli.ok("schedule", "describe", "nightly", "--json").out)
    assert data["spec"].get("cron") is None
    assert data["spec"]["interval"] == 7200
    assert data["policy"]["overlap"] == "allow_all"

    cli.ok("schedule", "update", "nightly", "--pause-on-failure")
    data = json.loads(cli.ok("schedule", "describe", "nightly", "--json").out)
    assert data["policy"]["pause_on_failure"] is True
    assert data["policy"]["overlap"] == "allow_all"

    cli.ok("schedule", "update", "nightly", "--no-pause-on-failure", "--note", "n")
    data = json.loads(cli.ok("schedule", "describe", "nightly", "--json").out)
    assert data["policy"]["pause_on_failure"] is False
    assert data["note"] == "n"


def test_update_without_changes_is_a_usage_error(cli: Cli) -> None:
    create(cli)
    result = cli("schedule", "update", "nightly")
    assert result.code == 2
    assert "nothing to change" in result.err


def test_delete(cli: Cli) -> None:
    create(cli)
    assert "deleted schedule nightly" in cli.ok("schedule", "delete", "nightly").err
    assert "no schedules" in cli.ok("schedule", "list").err
    missing = cli("schedule", "rm", "nightly")
    assert missing.code == 2


def test_describe_missing_schedule(cli: Cli) -> None:
    assert cli("schedule", "describe", "ghost").code == 3
