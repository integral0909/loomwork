"""Argument handling, exit codes and the real ``loomwork worker`` process."""

from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from loomwork._version import __version__
from loomwork.cli.commands.runtime import parse_queue_limit, worker_options
from loomwork.cli.errors import UsageError
from loomwork.cli.main import _split_global_flags, build_parser
from tests.cli.conftest import Cli

REPO = Path(__file__).resolve().parents[2]


def test_version(cli: Cli, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli("--version").code == 0
    assert capsys.readouterr().out.strip() == f"loomwork {__version__}"


def test_help_lists_every_command(cli: Cli, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli("--help").code == 0
    text = capsys.readouterr().out
    for command in (
        "workflow",
        "schedule",
        "worker",
        "server",
        "store",
        "compat",
        "lint",
        "cron",
        "token",
    ):
        assert command in text


def test_missing_command_is_a_usage_error(cli: Cli) -> None:
    result = cli()
    assert result.code == 2
    assert result.err.startswith("usage: loomwork")


def test_unknown_option_is_a_usage_error(cli: Cli) -> None:
    result = cli("workflow", "list", "--frobnicate")
    assert result.code == 2
    assert "unrecognized arguments: --frobnicate" in result.err


@pytest.mark.parametrize(
    ("argv", "expected", "json_output"),
    [
        (["--json", "wf", "ls"], ["wf", "ls"], True),
        (["wf", "ls", "--json"], ["wf", "ls"], True),
        (["wf", "start", "x", "--", "--json"], ["wf", "start", "x", "--", "--json"], False),
        (["lint", "a.py", "--json"], ["lint", "a.py", "--json"], False),
        (["--json", "lint", "a.py"], ["lint", "a.py"], True),
    ],
)
def test_json_flag_anywhere(argv: list[str], expected: list[str], json_output: bool) -> None:
    assert _split_global_flags(argv) == (expected, json_output)


def test_every_subcommand_has_a_handler() -> None:
    parser = build_parser()
    commands = {
        "workflow": [
            "start x",
            "list",
            "describe x",
            "history x",
            "result x",
            "signal x c 1",
            "cancel x",
            "terminate x --reason r",
            "steps x",
            "children x",
            "query x q --app m",
            "update x u",
        ],
        "schedule": [
            "create s w --every 1m",
            "list",
            "describe s",
            "pause s",
            "unpause s",
            "trigger s",
            "backfill s --from now",
            "upcoming s",
            "update s --note n",
            "delete s",
        ],
        "store": ["migrate", "info", "prune --older-than 1d"],
        "token": ["create t --role worker", "list", "revoke t", "verify"],
    }
    for group, lines in commands.items():
        for line in lines:
            args = parser.parse_args([group, *line.split()])
            assert callable(args.handler), line
    for line in [
        "worker --app m",
        "server -c x.toml",
        "compat --app m",
        "cron '* * * * *'",
        "lint x",
    ]:
        assert callable(parser.parse_args(shlex.split(line)).handler)


# --------------------------------------------------------------------------- worker options


def test_parse_queue_limit() -> None:
    queue, limit = parse_queue_limit("payments:rate=5,burst=10,concurrency=2")
    assert queue == "payments"
    assert (limit.rate, limit.burst, limit.concurrency) == (5, 10, 2)
    for bad in ["payments", "payments:", "p:speed=1", "p:rate=fast", "p:rate=-1"]:
        with pytest.raises(UsageError):
            parse_queue_limit(bad)


def test_worker_options_from_flags() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "worker",
            "--app",
            "m",
            "--step-concurrency",
            "8",
            "--queue",
            "default",
            "--queue",
            "gpu",
            "--queue-limit",
            "gpu:concurrency=1",
            "--no-schedules",
            "--shutdown-grace",
            "30s",
        ]
    )
    options = worker_options(args)
    assert options.step_concurrency == 8
    assert options.task_queues == ("default", "gpu")
    assert options.queue_limits["gpu"].concurrency == 1
    assert options.run_schedules is False
    assert options.run_timers is True
    assert options.shutdown_grace == 30


def test_queue_limits_imply_queues() -> None:
    args = build_parser().parse_args(["worker", "--app", "m", "--queue-limit", "gpu:rate=1"])
    assert worker_options(args).task_queues == ("default", "gpu")
    args = build_parser().parse_args(
        ["worker", "--app", "m", "--queue", "a", "--queue-limit", "b:rate=1"]
    )
    with pytest.raises(UsageError, match="not in --queue"):
        worker_options(args)


# --------------------------------------------------------------------------- real process


def _free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.mark.integration
@pytest.mark.slow
def test_worker_process_runs_workflows_and_stops_on_sigterm(cli: Cli, db_path: Path) -> None:
    port = _free_port()
    env = {**os.environ, "LOOMWORK_STORE_URL": cli.store_url, "PYTHONPATH": str(REPO)}
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "loomwork.cli",
            "worker",
            "--app",
            "tests.cli.sample_app",
            "--worker-id",
            "e2e",
            "--metrics-port",
            str(port),
            "--shutdown-grace",
            "2s",
        ],
        cwd=REPO,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        started = cli.ok(
            "workflow",
            "start",
            "cli.greet",
            '"proc"',
            "--id",
            "proc",
            "--wait",
            "--wait-timeout",
            "20s",
        )
        assert json.loads(started.out) == "HELLO PROC!"

        sample = (
            'loomwork_step_attempts_total{step="cli.shout",queue="default",outcome="completed"} 1'
        )
        metrics = ""
        deadline = time.monotonic() + 10
        while sample not in metrics and time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as r:
                    metrics = r.read().decode()
            except OSError:
                pass
            time.sleep(0.1)
        assert sample in metrics
        assert 'loomwork_ticks_total{workflow="cli.greet"' in metrics
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as r:
            assert r.status == 200

        process.send_signal(signal.SIGTERM)
        _, stderr = process.communicate(timeout=20)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert process.returncode == 0, stderr
    assert "worker e2e running 4 workflow(s) and 1 step(s)" in stderr
    assert "stopping" in stderr


def test_cli_works_without_the_optional_http_client() -> None:
    """httpx comes with the server/client extras; a core install must still run the CLI."""
    script = (
        "import sys\n"
        "sys.modules['httpx'] = None\n"  # makes any 'import httpx' raise ImportError
        "from loomwork.cli.main import main\n"
        "sys.exit(main(['cron', '0 3 * * *', '-n', '1', '--after', '2024-01-01']))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script], cwd=REPO, capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "2024-01-01T03:00:00+00:00"
