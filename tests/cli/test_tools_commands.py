"""``loomwork archive``, ``loomwork replay`` and ``loomwork workflow graph``."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

from loomwork.history import history_from_json
from tests.cli.conftest import APP, Cli, finish

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


# --------------------------------------------------------------------------- archive create


def test_archive_create_and_verify(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    archive_dir = tmp_path / "archive"

    result = cli.ok("archive", "create", "--older-than", "1ms", "--out", str(archive_dir))
    assert "archived 1 run(s) into 1 segment(s)" in result.out

    verified = cli.ok("archive", "verify", str(archive_dir))
    assert verified.out.strip() == "ok: 1 segment(s), 1 run(s)"


def test_archive_create_json_output(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    archive_dir = tmp_path / "archive"

    data = json.loads(
        cli.ok("archive", "create", "--older-than", "1ms", "--out", str(archive_dir), "--json").out
    )
    assert data["run_count"] == 1
    assert data["segments"] == 1
    assert data["pruned"] == 0


def test_archive_create_nothing_older_than_cutoff(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "s")
    archive_dir = tmp_path / "archive"

    result = cli.ok("archive", "create", "--older-than", "1d", "--out", str(archive_dir))
    assert "archived 0 run(s)" in result.out


def test_archive_create_with_prune_deletes_from_store(
    cli: Cli, db_path: Path, tmp_path: Path
) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    archive_dir = tmp_path / "archive"

    result = cli.ok(
        "archive", "create", "--older-than", "1ms", "--out", str(archive_dir), "--prune"
    )
    assert "pruned 1 run(s)" in result.err

    missing = cli("workflow", "describe", "a")
    assert missing.code == 3


def test_archive_create_rejects_query_with_prune(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    result = cli(
        "archive",
        "create",
        "--older-than",
        "1ms",
        "--out",
        str(tmp_path / "archive"),
        "--query",
        "status = 'failed'",
        "--prune",
    )
    assert result.code == 2
    assert "cannot be combined" in result.err


def test_archive_create_rejects_non_positive_age(cli: Cli, tmp_path: Path) -> None:
    result = cli("archive", "create", "--older-than", "0s", "--out", str(tmp_path / "a"))
    assert result.code == 2


def test_archive_create_query_filters_runs(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "ok-run")
    cli.ok("workflow", "start", "cli.fail", '"boom"', "--id", "bad-run")
    finish(db_path, "ok-run")
    finish(db_path, "bad-run")
    archive_dir = tmp_path / "archive"

    result = cli.ok(
        "archive",
        "create",
        "--older-than",
        "1ms",
        "--out",
        str(archive_dir),
        "--query",
        "status = 'failed'",
        "--json",
    )
    data = json.loads(result.out)
    assert data["run_count"] == 1
    listing = json.loads(cli.ok("archive", "list", str(archive_dir), "--json").out)
    assert [r["workflow_id"] for r in listing] == ["bad-run"]


# --------------------------------------------------------------------------- archive list/show


def test_archive_list_and_show(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    cli.ok("workflow", "start", "cli.fail", '"boom"', "--id", "b")
    finish(db_path, "a")
    finish(db_path, "b")
    archive_dir = tmp_path / "archive"
    cli.ok("archive", "create", "--older-than", "1ms", "--out", str(archive_dir))

    listing = cli.ok("archive", "list", str(archive_dir))
    assert "a" in listing.out
    assert "b" in listing.out

    only_greet = json.loads(
        cli.ok("archive", "list", str(archive_dir), "--workflow", "cli.greet", "--json").out
    )
    assert [r["workflow_id"] for r in only_greet] == ["a"]

    shown = cli.ok("archive", "show", str(archive_dir), "a")
    assert "workflow id:" in shown.out
    assert "a" in shown.out

    export_path = tmp_path / "exported.json"
    cli.ok("archive", "show", str(archive_dir), "b", "--export", str(export_path))
    events = history_from_json(export_path.read_text())
    assert events[0].kind == "workflow_started"


def test_archive_show_missing_run_is_not_found(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    archive_dir = tmp_path / "archive"
    cli.ok("archive", "create", "--older-than", "1ms", "--out", str(archive_dir))

    result = cli("archive", "show", str(archive_dir), "does-not-exist")
    assert result.code == 3


def test_archive_verify_detects_corruption(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    archive_dir = tmp_path / "archive"
    cli.ok("archive", "create", "--older-than", "1ms", "--out", str(archive_dir))
    segment = next(archive_dir.glob("segment-*.jsonl.gz"))
    data = bytearray(segment.read_bytes())
    data[len(data) // 2] ^= 0xFF
    segment.write_bytes(bytes(data))

    result = cli("archive", "verify", str(archive_dir))
    assert result.code == 1
    assert "CORRUPT" in result.out


def test_archive_verify_missing_manifest(cli: Cli, tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    result = cli("archive", "verify", str(empty))
    assert result.code == 1


# --------------------------------------------------------------------------- replay


def test_replay_history_file_ok(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    exported = tmp_path / "a.json"
    cli.ok("workflow", "history", "a", "-o", str(exported))

    result = cli.ok("replay", "--history", str(exported), "--app", APP)
    assert "TICK" in result.out.upper() or "tick" in result.out.lower()


def test_replay_reports_divergence(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    exported = tmp_path / "a.json"
    cli.ok("workflow", "history", "a", "-o", str(exported))
    app_file = tmp_path / "new_code.py"
    app_file.write_text(BROKEN_APP)

    result = cli("replay", "--history", str(exported), "--app", str(app_file))
    assert result.code == 1
    assert "DIVERGED" in result.out
    assert "shout" in result.out


def test_replay_from_archive(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")
    archive_dir = tmp_path / "archive"
    cli.ok("archive", "create", "--older-than", "1ms", "--out", str(archive_dir))
    run_id = json.loads(cli.ok("archive", "list", str(archive_dir), "--json").out)[0]["run_id"]

    result = cli.ok("replay", "--archive", str(archive_dir), "--run", run_id, "--app", APP)
    assert "shout" in result.out


def test_replay_from_store_by_workflow_id(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")

    result = cli.ok("replay", "a", "--app", APP)
    assert result.code == 0


def test_replay_until_tick_and_query(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.approval", '"widget"', "--id", "ap1")
    cli.ok("workflow", "signal", "ap1", "approvals", '"approved"')
    finish(db_path, "ap1")

    result = cli.ok(
        "replay", "ap1", "--app", APP, "--until-tick", "1", "--query", "state", "--json"
    )
    data = json.loads(result.out)
    assert data["stopped_at_tick"] == 1
    assert data["query_results"]["state"]["title"] == "widget"


def test_replay_requires_exactly_one_source(cli: Cli) -> None:
    result = cli("replay", "--app", APP)
    assert result.code == 2
    assert "give a history source" in result.err


def test_replay_history_and_archive_conflict(cli: Cli, tmp_path: Path) -> None:
    result = cli(
        "replay",
        "--history",
        str(tmp_path / "x.json"),
        "--archive",
        str(tmp_path / "y"),
        "--run",
        "r1",
        "--app",
        APP,
    )
    assert result.code == 2


def test_replay_archive_requires_run_id(cli: Cli, tmp_path: Path) -> None:
    result = cli("replay", "--archive", str(tmp_path / "a"), "--app", APP)
    assert result.code == 2
    assert "requires --run" in result.err


# --------------------------------------------------------------------------- workflow graph


def test_workflow_graph_text(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")

    result = cli.ok("workflow", "graph", "a", "--format", "text")
    assert "step" in result.out
    assert "shout" in result.out


def test_workflow_graph_mermaid(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")

    result = cli.ok("workflow", "graph", "a", "--format", "mermaid")
    assert result.out.startswith("gantt")


def test_workflow_graph_dot_json(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a")
    finish(db_path, "a")

    result = cli.ok("workflow", "graph", "a", "--format", "dot", "--json")
    data = json.loads(result.out)
    assert data["format"] == "dot"
    assert data["diagram"].startswith("digraph history {")
