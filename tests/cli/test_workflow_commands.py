"""``loomwork workflow ...`` against a real SQLite store and worker."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import loomwork as lw
from loomwork.history import history_from_json
from tests.cli.conftest import APP, Cli, drive, finish, run_with_worker


def test_start_prints_ids_and_run_completes(cli: Cli, db_path: Path) -> None:
    result = cli.ok("workflow", "start", "cli.greet", '"ada"', "--id", "greet-1")
    assert result.out.startswith("started greet-1 (run ")

    finish(db_path, "greet-1")
    described = cli.ok("workflow", "describe", "greet-1")
    assert "status:" in described.out
    assert "completed" in described.out
    assert '"HELLO ADA!"' in described.out or "HELLO ADA!" in described.out

    printed = cli.ok("workflow", "result", "greet-1")
    assert json.loads(printed.out) == "HELLO ADA!"


def test_start_json_output_and_arguments_from_file(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    (tmp_path / "name.json").write_text('"grace"', encoding="utf-8")
    result = cli.ok(
        "workflow", "start", "cli.greet", f"@{tmp_path / 'name.json'}", '"?"', "--id", "g", "--json"
    )
    data = json.loads(result.out)
    assert data["workflow_id"] == "g"
    assert data["run_id"]

    finish(db_path, "g")
    assert json.loads(cli.ok("wf", "result", "g").out) == "HELLO GRACE?"


def test_raw_arguments_are_strings(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", "lin", "--raw", "--id", "raw")
    finish(db_path, "raw")
    assert json.loads(cli.ok("workflow", "result", "raw").out) == "HELLO LIN!"


def test_arguments_from_stdin(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", "@-", "--id", "stdin", stdin='"piped"')
    finish(db_path, "stdin")
    assert json.loads(cli.ok("workflow", "result", "stdin").out) == "HELLO PIPED!"


def test_invalid_json_argument_is_a_usage_error(cli: Cli) -> None:
    result = cli("workflow", "start", "cli.greet", "not-json")
    assert result.code == 2
    assert "argument 1 is not valid JSON" in result.err


def test_duplicate_id_fails(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "dup")
    result = cli("workflow", "start", "cli.sleeper", "--id", "dup")
    assert result.code == 1
    assert "dup" in result.err


def test_result_of_running_run_needs_wait(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "sleepy")
    result = cli("workflow", "result", "sleepy")
    assert result.code == 1
    assert "still running" in result.err


def test_result_wait_timeout(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "sleepy")
    result = cli("workflow", "result", "sleepy", "--wait", "--wait-timeout", "50ms")
    assert result.code == 1
    assert "did not close within" in result.err


def test_failed_run_exits_one_and_shows_failure(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.fail", '"no budget"', "--id", "f")
    finish(db_path, "f")
    result = cli("workflow", "result", "f")
    assert result.code == 1
    assert result.out.strip() == "failed: Rejected: no budget"

    as_json = cli("workflow", "result", "f", "--json")
    data = json.loads(as_json.out)
    assert data["status"] == "failed"
    assert data["failure"]["type"] == "Rejected"

    described = cli.ok("workflow", "describe", "f")
    assert "failure:" in described.out
    assert "Rejected: no budget" in described.out


def test_list_filters_and_query(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"a"', "--id", "a", "--attr", "tier=gold")
    cli.ok("workflow", "start", "cli.greet", '"b"', "--id", "b", "--attr", "tier=silver")
    cli.ok("workflow", "start", "cli.sleeper", "--id", "s")
    finish(db_path, "a")
    finish(db_path, "b")

    everything = cli.ok("workflow", "list")
    assert everything.lines[0].split()[:3] == ["WORKFLOW", "ID", "RUN"]
    assert len(everything.lines) == 4

    running = cli.ok("workflow", "list", "--status", "running")
    assert [line.split()[0] for line in running.lines[1:]] == ["s"]

    by_name = cli.ok("wf", "ls", "--workflow", "cli.greet", "--json")
    assert sorted(r["workflow_id"] for r in json.loads(by_name.out)) == ["a", "b"]

    gold = cli.ok(
        "workflow", "list", "-q", "attr.tier = 'gold'", "--attr-type", "tier=string", "--json"
    )
    assert [r["workflow_id"] for r in json.loads(gold.out)] == ["a"]

    undeclared = cli("workflow", "list", "-q", "attr.tier = 'gold'")
    assert undeclared.code == 2
    assert "unknown search attribute 'attr.tier'" in undeclared.err


def test_list_query_conflicts_with_filters(cli: Cli) -> None:
    result = cli("workflow", "list", "-q", "status = 'running'", "--status", "failed")
    assert result.code == 2


def test_list_empty_says_so_on_stderr(cli: Cli) -> None:
    result = cli.ok("workflow", "list")
    assert result.out == ""
    assert "no runs found" in result.err


def test_describe_json_includes_search_attributes(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "attrs", "--attr", "n=3", "--attr", "t=x")
    data = json.loads(cli.ok("workflow", "describe", "attrs", "--json").out)
    assert data["search_attributes"] == {"n": 3, "t": "x"}
    assert data["status"] == "running"


def test_describe_unknown_is_not_found(cli: Cli) -> None:
    result = cli("workflow", "describe", "nope")
    assert result.code == 3


def test_describe_by_run_id(cli: Cli) -> None:
    started = json.loads(cli.ok("workflow", "start", "cli.sleeper", "--id", "r", "--json").out)
    data = json.loads(cli.ok("workflow", "describe", "--run", started["run_id"], "--json").out)
    assert data["workflow_id"] == "r"


def test_target_is_required(cli: Cli) -> None:
    result = cli("workflow", "describe")
    assert result.code == 2
    assert "workflow id or --run" in result.err


def test_history_table_json_and_export(cli: Cli, db_path: Path, tmp_path: Path) -> None:
    cli.ok("workflow", "start", "cli.greet", '"h"', "--id", "h")
    finish(db_path, "h")

    table = cli.ok("workflow", "history", "h")
    kinds = [line.split()[2] for line in table.lines[1:]]
    assert kinds[0] == "workflow_started"
    assert "step_scheduled" in kinds
    assert kinds[-1] == "workflow_completed"
    assert 'args=["h"]' in table.lines[1]

    only = json.loads(cli.ok("workflow", "history", "h", "--kind", "step_completed", "--json").out)
    assert [e["kind"] for e in only] == ["step_completed"]
    assert only[0]["result"] == "H"

    exported = cli.ok("workflow", "history", "h", "--export")
    events = history_from_json(exported.out)
    assert events[0].kind == "workflow_started"

    target = tmp_path / "h.json"
    written = cli.ok("workflow", "history", "h", "-o", str(target))
    assert written.out == ""
    assert f"to {target}" in written.err
    assert len(history_from_json(target.read_text())) == len(events)


def test_signal_query_and_update(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.approval", '"laptop"', "--id", "appr")

    async def first_tick(app: lw.Loomwork) -> None:
        # The first tick registers the query and update handlers.
        while (await app.get_handle("appr").describe()).tick_pending:  # noqa: ASYNC110
            await asyncio.sleep(0.02)

    drive(db_path, first_tick)

    state = cli.ok("workflow", "query", "appr", "state", "--app", APP)
    assert json.loads(state.out) == {"title": "laptop", "decisions": []}

    updated = run_with_worker(
        db_path, cli, "workflow", "update", "appr", "rename", '"desk"', "--wait-timeout", "10s"
    )
    assert updated.code == 0, updated.err
    assert json.loads(updated.out) == "laptop"

    sent = cli.ok("workflow", "signal", "appr", "approvals", '"yes"', "--message-id", "m1")
    assert "sent to channel 'approvals'" in sent.err
    finish(db_path, "appr")
    assert json.loads(cli.ok("workflow", "result", "appr").out) == "desk: yes"


def test_signal_raw_value(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.approval", '"x"', "--id", "raw-sig")
    cli.ok("workflow", "signal", "raw-sig", "approvals", "maybe", "--raw")
    finish(db_path, "raw-sig")
    assert json.loads(cli.ok("workflow", "result", "raw-sig").out) == "x: maybe"


def test_query_requires_app(cli: Cli) -> None:
    result = cli("workflow", "query", "x", "state")
    assert result.code == 2
    assert "--app" in result.err


def test_cancel_and_terminate(cli: Cli, db_path: Path) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "c")
    cancelled = cli.ok("workflow", "cancel", "c", "--reason", "not needed")
    assert "cancellation requested" in cancelled.err
    finish(db_path, "c")
    assert "canceled" in cli.ok("workflow", "describe", "c").out
    again = cli("workflow", "cancel", "c")
    assert again.code == 1

    cli.ok("workflow", "start", "cli.sleeper", "--id", "t")
    cli.ok("workflow", "terminate", "t", "--reason", "stuck")
    described = cli.ok("workflow", "describe", "t", "--json")
    data = json.loads(described.out)
    assert data["status"] == "terminated"
    assert data["close_reason"] == "stuck"
    result = cli("workflow", "result", "t")
    assert result.code == 1
    assert result.out.startswith("terminated")


def test_terminate_requires_reason(cli: Cli) -> None:
    assert cli("workflow", "terminate", "t").code == 2


def test_steps_and_children(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.greet", '"s"', "--id", "steps")
    assert "no pending steps" in cli.ok("workflow", "steps", "steps").err
    assert "no child workflows" in cli.ok("workflow", "children", "steps").err


def test_start_with_wait(cli: Cli, db_path: Path) -> None:
    result = run_with_worker(
        db_path, cli, "workflow", "start", "cli.greet", '"w"', "--id", "waited", "--wait"
    )
    assert result.code == 0, result.err
    assert json.loads(result.out) == "HELLO W!"
    assert "waiting" in result.err


def test_start_with_timeouts_and_retries(cli: Cli) -> None:
    cli.ok(
        "workflow",
        "start",
        "cli.sleeper",
        "--id",
        "bounded",
        "--run-timeout",
        "1h",
        "--execution-timeout",
        "2h",
        "--retry-attempts",
        "3",
    )
    history = json.loads(cli.ok("workflow", "history", "bounded", "--json").out)
    started = history[0]
    assert started["run_timeout"] == 3600
    assert started["retry_policy"]["max_attempts"] == 3


# --------------------------------------------------------------------------- build versioning


def test_start_with_versioning_policy(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "vp-1", "--versioning-policy", "pinned")
    data = json.loads(cli.ok("workflow", "describe", "vp-1", "--json").out)
    assert data["versioning_policy"] == "pinned"
    assert data["pinned_build_id"] is None


def test_repin_moves_a_pinned_run_and_unpin_clears_it(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "repin-1", "--versioning-policy", "pinned")
    result = cli.ok("workflow", "repin", "repin-1", "--to", "build-a")
    assert "build-a" in result.err

    data = json.loads(cli.ok("workflow", "describe", "repin-1", "--json").out)
    assert data["pinned_build_id"] == "build-a"

    cli.ok("workflow", "repin", "repin-1", "--unpin")
    data = json.loads(cli.ok("workflow", "describe", "repin-1", "--json").out)
    assert data["pinned_build_id"] is None


def test_repin_needs_a_build_or_unpin(cli: Cli) -> None:
    cli.ok("workflow", "start", "cli.sleeper", "--id", "repin-2")
    result = cli("workflow", "repin", "repin-2")
    assert result.code == 2  # argparse: mutually exclusive group is required
