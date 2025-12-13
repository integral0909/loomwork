"""Operator-facing worker-build operations.

Declaring compatibility, repinning a run and heartbeating a worker's build id are thin,
mechanical store calls (see :mod:`loomwork.store.base`). What operators actually want to
know -- "can this build be retired yet?", "is any pinned run stuck waiting for a build
that will never come back?" -- takes three of those calls combined, which is what
:class:`BuildVersioning` (``Engine.versioning`` / ``Client.versioning``) and
:func:`summarize_builds` are for. See ``docs/adr/0006-worker-build-versioning.md`` for
the design this supports and ``docs/operations/deployment.md`` for the operator workflow.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from loomwork.clock import Clock
from loomwork.store.base import Store
from loomwork.store.records import BuildCompatibility, WorkerPresence


@dataclass(frozen=True, slots=True)
class BuildSummary:
    """One build id's drain status."""

    build_id: str
    pinned_running_runs: int
    """Currently running executions pinned to this build. Zero means the build can be
    retired: no ``PINNED`` run will ever be claimed against it again."""
    live_workers: int
    """Workers currently heartbeating this exact build id (see
    :meth:`BuildVersioning.heartbeat`). Does not count workers of a *different* build
    that declared this one compatible -- see :attr:`orphaned`."""
    orphaned: bool
    """``pinned_running_runs`` is nonzero and no live worker -- of this build, or of one
    declared compatible with it -- could claim them. These runs are stuck: they will sit
    with a pending tick until a matching worker joins the fleet or an operator calls
    :meth:`BuildVersioning.repin`. Surface this to a metric or an alert; see
    ``docs/operations/monitoring.md``."""


def _can_serve(
    build_id: str, presence: list[WorkerPresence], compat: list[BuildCompatibility]
) -> bool:
    servers = {build_id} | {c.build_id for c in compat if c.compatible_with == build_id}
    return any(p.build_id in servers for p in presence)


def summarize_builds(
    counts: dict[str, int],
    presence: list[WorkerPresence],
    compat: list[BuildCompatibility],
) -> list[BuildSummary]:
    """Pure aggregation behind :meth:`BuildVersioning.builds_summary`, split out so a
    telemetry loop that already holds these three (cheap, already-fetched) values does
    not need another round trip just to check for orphaned builds."""
    build_ids = set(counts) | {p.build_id for p in presence}
    summaries = [
        BuildSummary(
            build_id=build_id,
            pinned_running_runs=counts.get(build_id, 0),
            live_workers=sum(1 for p in presence if p.build_id == build_id),
            orphaned=counts.get(build_id, 0) > 0 and not _can_serve(build_id, presence, compat),
        )
        for build_id in build_ids
    ]
    return sorted(summaries, key=lambda s: s.build_id)


class BuildVersioning:
    """Declares build compatibility, repins runs, and reports build drain status.

    Exposed as ``Engine.versioning`` and, identically, ``Client.versioning``. Every
    method is a thin wrapper over :class:`~loomwork.store.base.Store`; the only nontrivial
    logic (:meth:`builds_summary`'s orphan detection) is the pure :func:`summarize_builds`.
    """

    def __init__(self, store: Store, clock: Clock) -> None:
        self.store = store
        self.clock = clock

    async def declare_compatible(self, build_id: str, compatible_with: str) -> None:
        """Let workers of ``build_id`` claim ticks and pinned-queue steps pinned to
        ``compatible_with`` (for example a hotfix build that changed no workflow code,
        so it can safely drain the build it replaces). Directed and idempotent."""
        await self.store.declare_build_compatible(build_id, compatible_with, self.clock.now())

    async def revoke_compatible(self, build_id: str, compatible_with: str) -> bool:
        """Undo :meth:`declare_compatible`. Returns ``False`` if it was not declared."""
        return await self.store.revoke_build_compatibility(build_id, compatible_with)

    async def list_compatibility(self) -> list[BuildCompatibility]:
        return await self.store.list_build_compatibility()

    async def repin(self, run_id: str, build_id: str | None) -> None:
        """Move a run onto ``build_id`` (or, with ``None``, unpin it: its next tick pins
        fresh, exactly like a run that has never ticked). Use to rescue a run pinned to
        a build with no live or compatible workers left.

        Raises:
            NotFoundError: ``run_id`` does not exist.
            WorkflowClosedError: ``run_id`` has already closed.
        """
        await self.store.repin_run(run_id, build_id, self.clock.now())

    async def heartbeat(self, worker_id: str, build_id: str, ttl_seconds: float) -> None:
        """Record that ``worker_id`` (running ``build_id``) is alive for ``ttl_seconds``.

        Called by :class:`~loomwork.worker.worker.Worker` itself on a low-frequency loop
        (see :attr:`~loomwork.worker.worker.WorkerOptions.build_heartbeat_interval`); a
        direct caller is only needed for a process hosting workers outside of
        :class:`~loomwork.worker.worker.Worker` (for example custom tooling).
        """
        now = self.clock.now()
        await self.store.heartbeat_worker(
            worker_id, build_id, now, now + timedelta(seconds=ttl_seconds)
        )

    async def list_live_workers(self) -> list[WorkerPresence]:
        return await self.store.list_worker_builds(self.clock.now())

    async def builds_summary(self) -> list[BuildSummary]:
        """One row per build id seen in either a pinned run or a live worker's
        heartbeat. See :class:`BuildSummary` for what each field means and
        ``docs/operations/deployment.md`` for how to use it during a rolling deploy."""
        counts = await self.store.count_pinned_runs()
        presence = await self.list_live_workers()
        compat = await self.store.list_build_compatibility()
        return summarize_builds(counts, presence, compat)


__all__ = ["BuildSummary", "BuildVersioning", "summarize_builds"]
