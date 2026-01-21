"""Tests for ``examples.blue_green_deploy``."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from examples.blue_green_deploy import steps
from examples.blue_green_deploy.workflows import (
    PICK_WINDOW,
    WORKFLOW_NAME,
    process_widget_v1,
    process_widget_v2,
)
from examples.support import wait_for_query
from loomwork import Loomwork, ManualClock, WorkflowHandle
from loomwork.engine.config import EngineConfig
from loomwork.options import VersioningPolicy
from loomwork.store.sqlite import SQLiteStore
from loomwork.versioning import check_history
from loomwork.worker import WorkerOptions

BUILD_BLUE = "blue-1"
BUILD_BLUE_HOTFIX = "blue-1-hotfix"
BUILD_GREEN = "green-1"


def _config() -> EngineConfig:
    return EngineConfig(workflow_versioning_policy={WORKFLOW_NAME: VersioningPolicy.PINNED})


async def test_old_runs_stay_pinned_and_new_runs_go_to_the_new_build(
    tmp_path: Path, clock: ManualClock
) -> None:
    config = _config()
    async with SQLiteStore(tmp_path / "bg.db") as store:
        blue = Loomwork(
            store=store,
            workflows=[process_widget_v1],
            steps=[steps.reserve_widget, steps.ship_widget],
            clock=clock,
            config=config,
            worker_options=WorkerOptions(
                build_id=BUILD_BLUE, build_heartbeat_interval=30, shutdown_grace=0.5
            ),
        )
        async with blue:
            widget_1 = await blue.start_workflow(process_widget_v1, "w-1", id="w-1")
            await wait_for_query(
                clock, widget_1, "status", lambda s: s == "reserved", step=timedelta(0)
            )
            (blue_summary,) = await blue.client.versioning.builds_summary()
            assert blue_summary.pinned_running_runs == 1
            assert not blue_summary.orphaned

        # Blue is stopped: the run is pinned and mid-flight, waiting for a build that
        # can serve it. Give its presence time to lapse before checking "orphaned".
        await clock.advance(timedelta(seconds=100))
        async with Loomwork(store=store, config=config, run_worker=False) as readonly:
            (blue_summary,) = await readonly.client.versioning.builds_summary()
            assert blue_summary.live_workers == 0
            assert blue_summary.orphaned

        green = Loomwork(
            store=store,
            workflows=[process_widget_v2],
            steps=[steps.reserve_widget_v2, steps.ship_widget_fixed],
            clock=clock,
            config=config,
            worker_options=WorkerOptions(
                build_id=BUILD_GREEN, build_heartbeat_interval=3600, shutdown_grace=0.5
            ),
        )
        async with green:
            widget_2 = await green.start_workflow(process_widget_v2, "w-2", id="w-2")
            await wait_for_query(
                clock, widget_2, "status", lambda s: s == "reserved", step=timedelta(0)
            )
            summaries = {b.build_id: b for b in await green.client.versioning.builds_summary()}
            assert summaries[BUILD_GREEN].pinned_running_runs == 1
            # widget-1 is still pinned to blue and still orphaned: green cannot (and
            # must not) touch it, since green's registry cannot replay v1's history.
            assert summaries[BUILD_BLUE].orphaned

            hotfix = Loomwork(
                store=store,
                workflows=[process_widget_v1],
                steps=[steps.reserve_widget, steps.ship_widget_fixed],
                clock=clock,
                config=config,
                worker_options=WorkerOptions(
                    build_id=BUILD_BLUE_HOTFIX, build_heartbeat_interval=3600, shutdown_grace=0.5
                ),
            )
            async with hotfix:
                await hotfix.client.versioning.declare_compatible(BUILD_BLUE_HOTFIX, BUILD_BLUE)
                summaries = {b.build_id: b for b in await green.client.versioning.builds_summary()}
                assert not summaries[BUILD_BLUE].orphaned

                await clock.advance(PICK_WINDOW + timedelta(minutes=1))
                result_1 = await widget_1.result(timeout=5)
                result_2 = await widget_2.result(timeout=5)

                final = {b.build_id: b for b in await green.client.versioning.builds_summary()}
                assert BUILD_BLUE not in final  # fully drained; nothing left to report
                assert final[BUILD_GREEN].pinned_running_runs == 0

    # v1 ran to completion under the hotfixed step; v2 ran to completion with its own
    # (differently named) reservation step, unaffected by v1's history at all.
    assert result_1 == "track-res-w-1"
    assert result_2 == "track-res2-w-2"
    assert steps.reservations == [("v1", "res-w-1"), ("v2", "res2-w-2")]


async def test_the_old_and_new_workflow_code_each_replay_their_own_history_cleanly(
    tmp_path: Path, clock: ManualClock
) -> None:
    """No nondeterminism: each run's recorded history matches the code that (still)
    owns its build, however the two builds interleave along the way."""
    config = _config()
    steps_all: list[Any] = [steps.reserve_widget, steps.reserve_widget_v2, steps.ship_widget_fixed]
    async with SQLiteStore(tmp_path / "bg_replay.db") as store:
        blue = Loomwork(
            store=store,
            workflows=[process_widget_v1],
            steps=steps_all,
            clock=clock,
            config=config,
            worker_options=WorkerOptions(build_id=BUILD_BLUE, shutdown_grace=0.5),
        )
        green = Loomwork(
            store=store,
            workflows=[process_widget_v2],
            steps=steps_all,
            clock=clock,
            config=config,
            worker_options=WorkerOptions(build_id=BUILD_GREEN, shutdown_grace=0.5),
        )
        # Each run is created, and pinned, while only the build it should pin to is
        # running -- otherwise the other build's worker is equally eligible to claim
        # its (still unpinned) first tick, per docs/operations/deployment.md ("both are
        # eligible until a run is pinned"), which would make this test flaky.
        async with blue:
            old: WorkflowHandle[str] = await blue.start_workflow(process_widget_v1, "w-3", id="w-3")
            await wait_for_query(clock, old, "status", lambda s: s == "reserved", step=timedelta(0))

        async with green:
            new: WorkflowHandle[str] = await green.start_workflow(
                process_widget_v2, "w-4", id="w-4"
            )
            await wait_for_query(clock, new, "status", lambda s: s == "reserved", step=timedelta(0))

            async with blue:
                await clock.advance(PICK_WINDOW + timedelta(minutes=1))
                await old.result(timeout=5)
                await new.result(timeout=5)

            old_history = await store.load_history(old.run_id or "")
            new_history = await store.load_history(new.run_id or "")

    assert check_history(old_history, blue.registry).ok
    assert check_history(new_history, green.registry).ok
    # Cross-checking the *other* build's code against this history is exactly the
    # incompatible change worker build pinning exists to keep out of one build.
    assert not check_history(old_history, green.registry).ok
