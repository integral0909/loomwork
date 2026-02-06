"""The ``python -m benchmarks`` command line: run, list and compare benchmark scenarios.

.. code-block:: console

    uv run python -m benchmarks list
    uv run python -m benchmarks run --quick
    uv run python -m benchmarks run --store memory,sqlite,postgres --out results.json
    uv run python -m benchmarks compare old.json new.json --threshold 10%
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import cast

from benchmarks.harness import ALL_STORE_KINDS, StoreKind, run_case
from benchmarks.report import compare as compare_reports
from benchmarks.report import console_table, markdown_table
from benchmarks.results import SCHEMA_VERSION, Environment, ResultFile
from benchmarks.scenarios import SCENARIOS, SCENARIOS_BY_NAME
from benchmarks.stores import DEFAULT_POSTGRES_DSN

DEFAULT_STORES: tuple[StoreKind, ...] = ("memory", "sqlite")
DEFAULT_SEED = 1234
DEFAULT_THRESHOLD_PCT = 10.0


def _parse_stores(text: str) -> list[StoreKind]:
    kinds = [kind.strip() for kind in text.split(",") if kind.strip()]
    for kind in kinds:
        if kind not in ALL_STORE_KINDS:
            raise argparse.ArgumentTypeError(
                f"unknown store {kind!r}; choose from {', '.join(ALL_STORE_KINDS)}"
            )
    return cast(list[StoreKind], kinds)


def _parse_threshold(text: str) -> float:
    stripped = text.strip().removesuffix("%")
    try:
        return float(stripped)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a percentage: {text!r}") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m benchmarks", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run scenarios and report results")
    run_parser.add_argument(
        "--scenario",
        action="append",
        choices=sorted(SCENARIOS_BY_NAME),
        dest="scenarios",
        help="run only this scenario (repeatable); default: all",
    )
    run_parser.add_argument(
        "--store",
        type=_parse_stores,
        default=list(DEFAULT_STORES),
        help=f"comma-separated store backends (default: {','.join(DEFAULT_STORES)})",
    )
    run_parser.add_argument(
        "--quick",
        action="store_true",
        help="cheap parameters; memory/sqlite scenarios together finish in well under a minute",
    )
    run_parser.add_argument("--out", type=Path, help="write results as JSON to this path")
    run_parser.add_argument(
        "--seed", type=int, default=DEFAULT_SEED, help="base seed for workload data"
    )
    run_parser.add_argument(
        "--postgres-dsn",
        default=os.environ.get("LOOMWORK_BENCH_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
        help="DSN of an existing PostgreSQL cluster (a dedicated database is created in it)",
    )
    run_parser.add_argument(
        "--markdown", action="store_true", help="print a Markdown table instead of a console one"
    )

    subparsers.add_parser("list", help="list available scenarios")

    compare_parser = subparsers.add_parser("compare", help="diff two result files")
    compare_parser.add_argument("old", type=Path)
    compare_parser.add_argument("new", type=Path)
    compare_parser.add_argument(
        "--threshold",
        type=_parse_threshold,
        default=DEFAULT_THRESHOLD_PCT,
        help="regression threshold, e.g. '10%%' (default: 10%%)",
    )

    return parser


def _cmd_list() -> int:
    for scenario in SCENARIOS:
        print(f"{scenario.name:<20} [{scenario.metric:<10}] {scenario.description}")
        print(f"{'':<20}  stores: {', '.join(scenario.stores)}")
    return 0


def _cmd_compare(args: argparse.Namespace) -> int:
    old = ResultFile.load(args.old)
    new = ResultFile.load(args.new)
    report, has_regression = compare_reports(old, new, threshold_pct=args.threshold)
    print(report)
    return 1 if has_regression else 0


async def _run_all(args: argparse.Namespace) -> ResultFile:
    scenario_names = args.scenarios or [s.name for s in SCENARIOS]
    scenarios = [SCENARIOS_BY_NAME[name] for name in scenario_names]
    results = []
    with tempfile.TemporaryDirectory(prefix="loomwork-bench-") as tmp:
        work_dir = Path(tmp)
        for scenario in scenarios:
            for store_kind in args.store:
                if not scenario.supports(store_kind):
                    continue
                for case in scenario.cases(quick=args.quick):
                    result = await run_case(
                        scenario,
                        store_kind,
                        case,
                        base_seed=args.seed,
                        quick=args.quick,
                        work_dir=work_dir,
                        postgres_dsn=args.postgres_dsn,
                    )
                    print(
                        f"{result.scenario:<18} {result.store:<9} {result.case:<28} "
                        f"{result.point:>12.4g} {result.unit}"
                    )
                    results.append(result)
    return ResultFile(
        schema_version=SCHEMA_VERSION,
        environment=Environment.capture(),
        seed=args.seed,
        quick=args.quick,
        results=results,
    )


def _cmd_run(args: argparse.Namespace) -> int:
    result_file = asyncio.run(_run_all(args))
    print()
    print(markdown_table(result_file) if args.markdown else console_table(result_file))
    if args.out is not None:
        result_file.save(args.out)
        print(f"\nwrote {args.out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "list":
        return _cmd_list()
    if args.command == "compare":
        return _cmd_compare(args)
    return _cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())
