"""Worker build versioning: :class:`BuildVersioning`, :func:`summarize_builds`, and
their wiring into :class:`~loomwork.engine.engine.Engine` / :class:`~loomwork.client.
client.Client`. See ``docs/adr/0006-worker-build-versioning.md``."""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw
from loomwork import ManualClock, MemoryStore
from loomwork.engine.engine import Engine
from loomwork.options import VersioningPolicy
from loomwork.store.records import BuildCompatibility, TickOutcome, WorkerPresence
from loomwork.versioning.builds import BuildVersioning, summarize_builds
from tests.helpers import T0

# --------------------------------------------------------------------------- summarize_builds


class TestSummarizeBuilds:
    def test_no_data_is_empty(self) -> None:
        assert summarize_builds({}, [], []) == []

    def test_build_with_workers_and_no_pinned_runs_is_not_orphaned(self) -> None:
        presence = [WorkerPresence("w1", "b1", T0, T0 + timedelta(seconds=30))]
        summaries = summarize_builds({}, presence, [])
        assert len(summaries) == 1
        assert summaries[0].build_id == "b1"
        assert summaries[0].pinned_running_runs == 0
        assert summaries[0].live_workers == 1
        assert not summaries[0].orphaned

    def test_pinned_runs_with_a_live_matching_worker_is_not_orphaned(self) -> None:
        presence = [WorkerPresence("w1", "b1", T0, T0 + timedelta(seconds=30))]
        summaries = summarize_builds({"b1": 3}, presence, [])
        assert summaries[0].pinned_running_runs == 3
        assert not summaries[0].orphaned

    def test_pinned_runs_with_no_live_worker_is_orphaned(self) -> None:
        summaries = summarize_builds({"b1": 2}, [], [])
        assert summaries[0].orphaned
        assert summaries[0].live_workers == 0

    def test_a_compatible_builds_worker_prevents_orphaning(self) -> None:
        presence = [WorkerPresence("w1", "b2", T0, T0 + timedelta(seconds=30))]
        compat = [BuildCompatibility("b2", "b1", T0)]
        summaries = summarize_builds({"b1": 2}, presence, compat)
        by_id = {s.build_id: s for s in summaries}
        assert not by_id["b1"].orphaned
        # The compatibility is directed: b2 does not gain b1's live_workers count.
        assert by_id["b2"].live_workers == 1

    def test_compatibility_is_directed_and_does_not_prevent_the_reverse(self) -> None:
        # b2 declared compatible with b1 does not help b2's own pinned runs if nothing
        # is compatible *with* b2.
        compat = [BuildCompatibility("b2", "b1", T0)]
        summaries = summarize_builds({"b2": 1}, [], compat)
        assert summaries[0].orphaned

    def test_results_are_sorted_by_build_id(self) -> None:
        presence = [
            WorkerPresence("w1", "z-build", T0, T0 + timedelta(seconds=30)),
            WorkerPresence("w2", "a-build", T0, T0 + timedelta(seconds=30)),
        ]
        summaries = summarize_builds({}, presence, [])
        assert [s.build_id for s in summaries] == ["a-build", "z-build"]


# --------------------------------------------------------------------------- BuildVersioning


class TestBuildVersioning:
    async def test_declare_list_and_revoke_compatibility(self) -> None:
        store = MemoryStore()
        versioning = BuildVersioning(store, ManualClock(T0))
        await versioning.declare_compatible("b2", "b1")
        items = await versioning.list_compatibility()
        assert [(i.build_id, i.compatible_with) for i in items] == [("b2", "b1")]
        assert await versioning.revoke_compatible("b2", "b1")
        assert await versioning.list_compatibility() == []

    async def test_heartbeat_and_list_live_workers(self) -> None:
        clock = ManualClock(T0)
        store = MemoryStore()
        versioning = BuildVersioning(store, clock)
        await versioning.heartbeat("w1", "b1", ttl_seconds=30)
        live = await versioning.list_live_workers()
        assert [(p.worker_id, p.build_id) for p in live] == [("w1", "b1")]
        await clock.advance(timedelta(seconds=31))
        assert await versioning.list_live_workers() == []

    async def test_repin_moves_a_run(self) -> None:
        clock = ManualClock(T0)
        store = MemoryStore()
        engine = Engine(store, clock=clock)
        record = await engine.start_workflow("wf", [], versioning_policy=VersioningPolicy.PINNED)
        claimed = await store.claim_tick("w1", T0, timedelta(seconds=30), build_id="build-a")
        assert claimed is not None
        await store.complete_tick(
            record.run_id,
            "w1",
            TickOutcome(worker_id="w1", consumed_through=1, time=T0, build_id="build-a"),
            T0,
        )
        assert (await store.get_execution(record.run_id)).pinned_build_id == "build-a"
        await engine.versioning.repin(record.run_id, "build-b")
        assert (await store.get_execution(record.run_id)).pinned_build_id == "build-b"

    async def test_builds_summary_reports_orphaned_build(self) -> None:
        clock = ManualClock(T0)
        store = MemoryStore()
        engine = Engine(store, clock=clock)
        record = await engine.start_workflow("wf", [], versioning_policy=VersioningPolicy.PINNED)
        await store.claim_tick("w1", T0, timedelta(seconds=30), build_id="stale-build")
        await store.complete_tick(
            record.run_id,
            "w1",
            TickOutcome(worker_id="w1", consumed_through=1, time=T0, build_id="stale-build"),
            T0,
        )
        summary = {s.build_id: s for s in await engine.versioning.builds_summary()}
        assert summary["stale-build"].orphaned
        await engine.versioning.heartbeat("w2", "stale-build", 30)
        summary = {s.build_id: s for s in await engine.versioning.builds_summary()}
        assert not summary["stale-build"].orphaned


# --------------------------------------------------------------------------- wiring


async def test_client_versioning_is_the_engine_versioning_instance() -> None:
    async with lw.Loomwork(store=MemoryStore(), run_worker=False) as app:
        assert app.client.versioning is app.engine.versioning
