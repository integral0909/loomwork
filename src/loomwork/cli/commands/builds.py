"""``loomwork builds ...``: operate worker build versioning (see
``docs/guides/versioning.md`` and ``docs/adr/0006-worker-build-versioning.md``).

Goes through :func:`~loomwork.cli.backend.open_operations` and its
:attr:`~loomwork.cli.backend.Operations.versioning` attribute, exactly like
``workflow repin`` does -- there is no separate backend-selection function for builds,
since a build id is not tied to one workflow id the way runs are.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from typing import Any

from loomwork.cli.backend import open_operations
from loomwork.cli.commands.common import connected
from loomwork.cli.errors import ExitCode, UsageError
from loomwork.cli.output import Console

_HEADERS = ["build", "live workers", "pinned running runs", "orphaned", "declared compatible with"]


async def list_builds(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        summaries = await ops.versioning.builds_summary()
        compat = await ops.versioning.list_compatibility()
    compatible_with: dict[str, list[str]] = defaultdict(list)
    for c in compat:
        compatible_with[c.build_id].append(c.compatible_with)
    rows = [
        [
            s.build_id,
            s.live_workers,
            s.pinned_running_runs,
            s.orphaned,
            ", ".join(sorted(compatible_with.get(s.build_id, []))) or None,
        ]
        for s in summaries
    ]
    records = [
        {
            "build_id": s.build_id,
            "live_workers": s.live_workers,
            "pinned_running_runs": s.pinned_running_runs,
            "orphaned": s.orphaned,
            "compatible_with": sorted(compatible_with.get(s.build_id, [])),
        }
        for s in summaries
    ]
    console.table(_HEADERS, rows, records=records, empty="no build has live workers or pinned runs")
    return ExitCode.OK


async def workers(args: argparse.Namespace, console: Console) -> int:
    async with open_operations(args) as ops:
        presence = await ops.versioning.list_live_workers()
    rows = [[w.worker_id, w.build_id, w.last_seen_at, w.expires_at] for w in presence]
    console.table(
        ["worker", "build", "last seen", "expires"], rows, records=presence, empty="no live workers"
    )
    return ExitCode.OK


async def compatible(args: argparse.Namespace, console: Console) -> int:
    if not args.with_build:
        raise UsageError("--with BUILD is required")
    async with open_operations(args) as ops:
        if args.revoke:
            revoked = await ops.versioning.revoke_compatible(args.build_id, args.with_build)
            if not revoked:
                raise UsageError(
                    f"{args.build_id!r} was not declared compatible with {args.with_build!r}"
                )
            console.note(f"revoked: {args.build_id} is no longer compatible with {args.with_build}")
        else:
            await ops.versioning.declare_compatible(args.build_id, args.with_build)
            console.note(
                f"declared: {args.build_id} may now claim work pinned to {args.with_build}"
            )
    return ExitCode.OK


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser("builds", help="operate worker build versioning")
    commands = parser.add_subparsers(dest="builds_command", metavar="COMMAND", required=True)

    p = commands.add_parser(
        "list", aliases=["ls"], help="drain status of every build seen in a pin or a heartbeat"
    )
    connected(p, list_builds)

    p = commands.add_parser("workers", help="workers currently heartbeating a build id")
    connected(p, workers)

    p = commands.add_parser(
        "compatible", help="declare (or revoke) that a build may claim another's pinned work"
    )
    p.add_argument("build_id", help="the build that gains eligibility")
    p.add_argument(
        "--with",
        dest="with_build",
        metavar="OLD_BUILD",
        help="the build it becomes compatible with",
    )
    p.add_argument(
        "--revoke", action="store_true", help="undo a previous declaration instead of making one"
    )
    connected(p, compatible)
