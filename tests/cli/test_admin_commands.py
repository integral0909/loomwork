"""``loomwork store``, ``compat``, ``lint`` and ``cron``."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from loomwork.store.sql import MIGRATIONS
from tests.cli.conftest import APP, Cli, finish

NEWEST_MIGRATION = max(m.version for m in MIGRATIONS)


# --------------------------------------------------------------------------- store


def test_migrate_reports_schema_version(cli: Cli) -> None:
    result = cli.ok("store", "migrate")
    assert result.out.strip() == f"schema is at version {NEWEST_MIGRATION}"
    as_json = json.loads(cli.ok("store", "migrate", "--json").out)
    assert as_json == {"schema_version": NEWEST_MIGRATION}


def test_migrate_memory_store_has_no_schema(cli: Cli) -> None:
    result = cli.ok("store", "migrate", "--store", "memory://")
    assert "no schema" in result.err


def test_info(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "s")
    cli.ok("workflow", "start", "cli.fail", '"x"', "--id", "f")
    finish(db_path, "f")
    info = cli.ok("store", "info")
    assert "SQLiteStore" in info.out
    assert f"schema version: {NEWEST_MIGRATION}" in " ".join(info.out.split())
    fields = dict(
        line.split(":", 1) for line in info.lines if ":" in line and not line.startswith("store")
    )
    assert fields["running runs"].strip() == "1"
    assert fields["failed runs"].strip() == "1"


def test_prune_dry_run_then_delete(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"old"', "--id", "old")
    cli.ok("workflow", "start", "cli.sleeper", "--id", "running")
    finish(db_path, "old")

    # Nothing closed more than a day ago.
    assert (
        "0 run(s) closed before" in cli.ok("store", "prune", "--older-than", "1d", "--dry-run").err
    )

    # Everything closed before "one millisecond ago" (i.e. now).
    dry = cli.ok("store", "prune", "--older-than", "1ms", "--dry-run", "--json")
    data = json.loads(dry.out)
    assert data["would_delete"] == 1
    assert data["runs"][0]["workflow_id"] == "old"

    deleted = cli.ok("store", "prune", "--older-than", "1ms")
    assert deleted.out.startswith("deleted 1 run(s)")
    remaining = json.loads(cli.ok("workflow", "list", "--json").out)
    assert [r["workflow_id"] for r in remaining] == ["running"]


def test_prune_rejects_zero_age(cli: Cli) -> None:
    assert cli("store", "prune", "--older-than", "0s").code == 2


def test_missing_store_configuration(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOMWORK_STORE_URL")
    result = cli("store", "info")
    assert result.code == 2
    assert "no store configured" in result.err


def test_invalid_store_url(cli: Cli) -> None:
    result = cli("store", "info", "--store", "mysql://nope")
    assert result.code == 2
    assert "invalid store URL" in result.err


# --------------------------------------------------------------------------- compat


BROKEN_APP = textwrap.dedent(
    """
    import loomwork as lw

    @lw.step(name="cli.shout")
    async def shout(text: str) -> str:
        return text.upper()

    @lw.step(name="cli.whisper")
    async def whisper(text: str) -> str:
        return text.lower()

    @lw.workflow(name="cli.greet")
    async def greet(name: str, punctuation: str = "!") -> str:
        quiet = await whisper(name)          # was: shout(name)
        return f"hello {quiet}{punctuation}"
    """
)


def test_compat_passes_for_unchanged_code(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    result = cli.ok("compat", "--app", APP, "--all")
    assert "checked 1 run(s): 1 ok, 0 broken" in result.err
    assert "ok" in result.out


def test_compat_detects_incompatible_change(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    cli.ok("workflow", "start", "cli.sleeper", "--id", "s")
    finish(db_path, "a")
    app_file = tmp_path / "new_code.py"
    app_file.write_text(BROKEN_APP)

    result = cli("compat", "--app", str(app_file))
    assert result.code == 1
    assert "BROKEN" in result.out
    assert "1 broken" in result.err
    assert "skipped 1 run(s) of 'cli.sleeper'" in result.err

    as_json = json.loads(cli("compat", "--app", str(app_file), "--json").out)
    assert as_json["broken"] == 1
    assert as_json["runs"][0]["nondeterministic"] is True


def test_compat_on_exported_history_files(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    exported = tmp_path / "a.json"
    cli.ok("workflow", "history", "a", "-o", str(exported))
    app_file = tmp_path / "new_code.py"
    app_file.write_text(BROKEN_APP)

    assert cli.ok("compat", "--app", APP, "--history", str(exported)).code == 0
    assert cli("compat", "--app", str(app_file), "--history", str(exported)).code == 1


def test_compat_with_unknown_app(cli: Cli) -> None:
    result = cli("compat", "--app", "tests.cli.does_not_exist")
    assert result.code == 2
    assert "cannot import module" in result.err


# --------------------------------------------------------------------------- lint


def test_lint_passes_arguments_through(cli: Cli, tmp_path: Path) -> None:
    clean = tmp_path / "clean.py"
    clean.write_text(
        "import loomwork as lw\n\n@lw.workflow\nasync def ok() -> int:\n    return 1\n"
    )
    dirty = tmp_path / "dirty.py"
    dirty.write_text(
        "import time\nimport loomwork as lw\n\n"
        "@lw.workflow\nasync def bad() -> float:\n    return time.time()\n"
    )
    assert cli("lint", str(clean)).code == 0
    # --json after "lint" belongs to the linter, which has its own --format flag.
    result = cli("lint", str(dirty), "--format", "json")
    assert result.code == 1


# --------------------------------------------------------------------------- cron


def test_cron_next_times_in_zone(cli: Cli) -> None:
    result = cli.ok(
        "cron", "30 2 * * *", "--tz", "Europe/Berlin", "-n", "2", "--after", "2024-03-30T12:00:00Z"
    )
    # 02:30 does not exist on 2024-03-31 in Berlin (clocks jump from 02:00 to 03:00): the
    # fire time moves forward by the length of the gap.
    assert result.lines == ["2024-03-31T03:30:00+02:00", "2024-04-01T02:30:00+02:00"]


def test_cron_json(cli: Cli) -> None:
    result = cli.ok("cron", "@hourly", "-n", "2", "--after", "2024-01-01T00:10:00Z", "--json")
    data = json.loads(result.out)
    assert data["next"] == ["2024-01-01T01:00:00+00:00", "2024-01-01T02:00:00+00:00"]


def test_cron_invalid(cli: Cli) -> None:
    result = cli("cron", "* * *")
    assert result.code == 2
    assert "invalid cron expression" in result.err


def test_lint_help_reaches_the_linter(cli: Cli, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli("lint", "--help").code == 0
    assert capsys.readouterr().out.startswith("usage: loomwork lint")


def test_lint_after_global_log_level(cli: Cli, tmp_path: Path) -> None:
    clean = tmp_path / "clean.py"
    clean.write_text("x = 1\n")
    assert cli("--log-level", "debug", "lint", str(clean)).code == 0
