"""The ``loomwork`` command.

``main()`` parses arguments, runs one command handler on a fresh event loop and turns
every expected error into one line on stderr and a meaningful exit code (see
:class:`~loomwork.cli.errors.ExitCode`). Unexpected errors keep their traceback: they are
bugs, and hiding them would make them harder to report.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence
from typing import TextIO

from loomwork._version import __version__
from loomwork.cli.commands import admin, runtime, schedule, token, workflow
from loomwork.cli.errors import CliError, ExitCode
from loomwork.cli.output import Console
from loomwork.errors import (
    LoomworkError,
    NotFoundError,
    ScheduleExistsError,
    WorkflowAlreadyStartedError,
)

#: Commands whose own arguments are passed through untouched.
_PASSTHROUGH = frozenset({"lint"})


class _Parser(argparse.ArgumentParser):
    """Reports usage errors through :class:`CliError` instead of exiting."""

    def error(self, message: str) -> None:  # type: ignore[override]
        raise _ParseError(self.format_usage(), message)


class _ParseError(Exception):
    def __init__(self, usage: str, message: str) -> None:
        super().__init__(message)
        self.usage = usage
        self.message = message


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="loomwork",
        description="Operate Loomwork: run workers and the server, start and inspect "
        "workflows, manage schedules, tokens and stores.",
        epilog="Connection flags (--store, --server, ...) default to the LOOMWORK_* "
        "environment variables. Add --json to any command for machine-readable output.",
    )
    parser.add_argument("--version", action="version", version=f"loomwork {__version__}")
    parser.add_argument(
        "--json", action="store_true", help="print machine-readable JSON instead of tables"
    )
    parser.add_argument(
        "--log-level",
        default="warning",
        choices=["debug", "info", "warning", "error"],
        help="log level for Loomwork's own logs (default: warning; info for worker/server)",
    )
    subparsers = parser.add_subparsers(
        dest="command", metavar="COMMAND", required=True, parser_class=_Parser
    )
    workflow.register(subparsers)
    schedule.register(subparsers)
    runtime.register(subparsers)
    admin.register(subparsers)
    token.register(subparsers)
    return parser


def _split_global_flags(argv: list[str]) -> tuple[list[str], bool]:
    """Accept ``--json`` anywhere on the line, not only before the command, since that is
    where people type it. Arguments after ``--`` and those of pass-through commands are
    left alone."""
    json_output = False
    result: list[str] = []
    passthrough = False
    for index, arg in enumerate(argv):
        if passthrough or arg == "--":
            result.extend(argv[index:])
            break
        if arg == "--json":
            json_output = True
            continue
        if arg in _PASSTHROUGH and not any(a in _PASSTHROUGH for a in result):
            passthrough = True
        result.append(arg)
    return result, json_output


def _passthrough(arguments: list[str]) -> tuple[str, list[str]] | None:
    """``(command, rest)`` when the command hands its arguments to another parser.

    argparse cannot forward arguments that start with ``-`` (``loomwork lint --help``) to a
    ``REMAINDER`` positional, so these commands are split off before it runs.
    """
    index = 0
    while index < len(arguments):
        arg = arguments[index]
        if arg == "--log-level":
            index += 2
            continue
        if arg.startswith("-"):
            return None
        return (arg, arguments[index + 1 :]) if arg in _PASSTHROUGH else None
    return None


def _configure_logging(level: str, command: str) -> None:
    if command in {"worker", "server"} and level == "warning":
        level = "info"
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _is_transport_error(exc: BaseException) -> bool:
    try:
        import httpx
    except ImportError:  # pragma: no cover - httpx is optional
        return False
    return isinstance(exc, httpx.TransportError)


def _describe(exc: BaseException) -> tuple[str, ExitCode] | None:
    """Message and exit code for errors users are expected to run into."""
    if isinstance(exc, CliError):
        return exc.message, exc.code
    if isinstance(exc, NotFoundError):
        return str(exc), ExitCode.NOT_FOUND
    if isinstance(exc, WorkflowAlreadyStartedError | ScheduleExistsError):
        return str(exc), ExitCode.FAILURE
    if isinstance(exc, LoomworkError):
        return f"{type(exc).__name__}: {exc}", ExitCode.FAILURE
    if _is_transport_error(exc):
        return f"cannot reach the server: {exc}", ExitCode.UNAVAILABLE
    from loomwork.store.sql import MigrationError

    if isinstance(exc, MigrationError):
        return str(exc), ExitCode.FAILURE
    from loomwork.schedules.model import ScheduleError
    from loomwork.visibility.errors import VisibilityQueryError

    if isinstance(exc, VisibilityQueryError):
        return f"invalid query: {exc}", ExitCode.USAGE

    if isinstance(exc, ScheduleError):
        return str(exc), ExitCode.USAGE
    if isinstance(exc, ConnectionError):
        return f"connection failed: {exc}", ExitCode.UNAVAILABLE
    return None


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    stdin: TextIO | None = None,
) -> int:
    """Run the CLI and return its exit code; never calls ``sys.exit``."""
    arguments, json_output = _split_global_flags(list(argv) if argv is not None else sys.argv[1:])
    console = Console(stdout, stderr, stdin=stdin)
    forwarded = _passthrough(arguments)
    if forwarded is not None:
        from loomwork.lint.cli import main as lint_main

        return lint_main(forwarded[1], prog="loomwork lint")
    parser = build_parser()
    try:
        args = parser.parse_args(arguments)
    except _ParseError as exc:
        console.err.write(exc.usage)
        console.error(exc.message)
        return ExitCode.USAGE
    except SystemExit as exc:  # --help and --version
        return int(exc.code or 0)
    console.json_output = json_output or args.json
    _configure_logging(args.log_level, args.command)

    try:
        return int(asyncio.run(args.handler(args, console)))
    except KeyboardInterrupt:
        console.note("interrupted")
        return ExitCode.INTERRUPTED
    except Exception as exc:
        described = _describe(exc)
        if described is None:
            raise
        message, code = described
        console.error(message)
        return code


def run() -> None:
    """Console-script entry point."""
    sys.exit(main())
