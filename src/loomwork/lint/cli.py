"""The ``python -m loomwork.lint`` command line interface.

Exit codes match the convention used by most static analyzers: ``0`` when the scan found
nothing to report, ``1`` when it found findings (the workflows it looked at are not
clean), and ``2`` when the invocation itself was wrong (bad arguments, a path that does
not exist, invalid configuration) - a distinction CI pipelines rely on to tell "your code
has a problem" apart from "your command was wrong".

``loomwork lint ...`` passes its arguments straight to :func:`main`.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from loomwork.lint.analyzer import lint_paths
from loomwork.lint.config import LintConfig
from loomwork.lint.errors import ConfigError, LintUsageError
from loomwork.lint.registry import DEFAULT_REGISTRY
from loomwork.lint.reporters import REPORTERS

_DEFAULT_PYPROJECT = Path("pyproject.toml")


class _ArgumentParser(argparse.ArgumentParser):
    """Raises instead of calling ``sys.exit`` so ``main()`` stays a plain function."""

    def error(self, message: str) -> None:  # type: ignore[override]
        raise LintUsageError(message)


def _build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="python -m loomwork.lint",
        description="Static determinism checks for Loomwork workflow functions.",
    )
    parser.add_argument("paths", nargs="+", help="files or directories to scan")
    parser.add_argument(
        "--format",
        "-f",
        choices=sorted(REPORTERS),
        default="text",
        help="output format (default: text)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="path to a pyproject.toml to read [tool.loomwork.lint] from "
        "(default: ./pyproject.toml if present)",
    )
    parser.add_argument(
        "--select",
        default=None,
        help="comma-separated rule codes to check exclusively, overriding config",
    )
    parser.add_argument(
        "--ignore",
        default=None,
        help="comma-separated rule codes to add to the ignore list, in addition to config",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="write the report to this file instead of stdout",
    )
    return parser


def _parse_codes(raw: str, *, option: str) -> frozenset[str]:
    codes = frozenset(c.strip() for c in raw.split(",") if c.strip())
    unknown = sorted(codes - DEFAULT_REGISTRY.codes())
    if unknown:
        raise LintUsageError(f"unknown rule code(s) in {option}: {', '.join(unknown)}")
    return codes


def _load_config(args: argparse.Namespace) -> LintConfig:
    config_path = args.config if args.config is not None else _DEFAULT_PYPROJECT
    config = LintConfig.from_pyproject(config_path)
    if args.select is not None:
        config = LintConfig(
            select=_parse_codes(args.select, option="--select"),
            ignore=config.ignore,
            extend_forbidden_calls=config.extend_forbidden_calls,
            allowed_calls=config.allowed_calls,
            exclude=config.exclude,
        )
    if args.ignore is not None:
        config = LintConfig(
            select=config.select,
            ignore=config.ignore | _parse_codes(args.ignore, option="--ignore"),
            extend_forbidden_calls=config.extend_forbidden_calls,
            allowed_calls=config.allowed_calls,
            exclude=config.exclude,
        )
    return config


def main(argv: Sequence[str] | None = None) -> int:
    """Run the linter. Returns the process exit code; never calls ``sys.exit``."""
    parser = _build_parser()
    stream = sys.stdout
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
        config = _load_config(args)
        result = lint_paths([str(p) for p in args.paths], config)
    except (LintUsageError, ConfigError) as exc:
        sys.stderr.write(f"loomwork.lint: error: {exc}\n")
        return 2

    report = REPORTERS[args.format](result)
    if args.output is not None:
        args.output.write_text(report, encoding="utf-8")
    else:
        stream.write(report)
    return result.exit_code()
