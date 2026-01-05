"""Command-line entry point for long soak runs.

.. code-block:: console

    python -m loomwork.testing.simulation --seeds 1..200 --store sqlite

Runs every seed in the range to completion, one after another, printing a one-line
summary per seed and the full report for any seed that finds a violation. Exits non-zero
if any seed failed, so it composes with CI or a scheduled soak job.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Iterator

from loomwork.cli.output import Console
from loomwork.testing.simulation.harness import SimulationConfig, simulate


def _parse_seeds(spec: str) -> Iterator[int]:
    """``"7"`` -> one seed; ``"1..200"`` -> the inclusive range 1 through 200."""
    if ".." in spec:
        start, _, end = spec.partition("..")
        yield from range(int(start), int(end) + 1)
    else:
        yield int(spec)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m loomwork.testing.simulation",
        description="Run deterministic simulation seeds and report any invariant violation.",
    )
    parser.add_argument(
        "--seeds",
        default="1..50",
        help="a single seed ('7') or an inclusive range ('1..200'); default: 1..50",
    )
    parser.add_argument("--store", choices=["memory", "sqlite"], default="memory")
    parser.add_argument("--num-runs", type=int, default=20)
    parser.add_argument("--num-workers", type=int, default=3)
    parser.add_argument("--fault-rate", type=float, default=0.1)
    parser.add_argument("--lease-pressure", type=float, default=0.05)
    parser.add_argument("--kill-rate", type=float, default=0.3)
    parser.add_argument("--cache-evict-rate", type=float, default=0.3)
    parser.add_argument(
        "--wall-clock-timeout",
        type=float,
        default=20.0,
        help="real seconds each run gets to close before it is a liveness violation",
    )
    parser.add_argument("--quiet", action="store_true", help="print only failing seeds")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help=(
            "show the engine's own logging (every injected fault is a handled, "
            "expected error the worker logs and recovers from; noisy by design)"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    if not args.verbose:
        # A soak run injects faults on purpose; the worker logging each one as an error
        # it recovered from is correct but drowns out the seed-by-seed summary below.
        logging.getLogger("loomwork").setLevel(logging.CRITICAL)
    console = Console()
    config = SimulationConfig(
        store=args.store,
        num_runs=args.num_runs,
        num_workers=args.num_workers,
        fault_rate=args.fault_rate,
        lease_pressure=args.lease_pressure,
        kill_rate=args.kill_rate,
        cache_evict_rate=args.cache_evict_rate,
        wall_clock_timeout=args.wall_clock_timeout,
    )
    seeds = list(_parse_seeds(args.seeds))
    failures = 0
    for seed in seeds:
        report = simulate(seed, config)
        failed = not report.ok
        failures += failed
        if failed or not args.quiet:
            status = "OK" if report.ok else "FAILED"
            console.print(
                f"seed {seed}: {status} ({report.closed}/{report.started} closed, "
                f"{report.duration_seconds:.2f}s)"
            )
        if failed:
            console.print(report.format_text())
            console.print()
    console.print(f"\n{len(seeds) - failures}/{len(seeds)} seeds passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
