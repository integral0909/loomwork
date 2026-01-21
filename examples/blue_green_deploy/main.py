"""Two worker builds of one pinned workflow, side by side against one SQLite store.

.. code-block:: console

    uv run python -m examples.blue_green_deploy.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import timedelta
from pathlib import Path

import loomwork as lw
from examples.blue_green_deploy import steps
from examples.blue_green_deploy.workflows import (
    PICK_WINDOW,
    WORKFLOW_NAME,
    process_widget_v1,
    process_widget_v2,
)
from examples.support import DEMO_POLL_INTERVAL, rule, wait_for_query
from loomwork import ManualClock
from loomwork.engine.config import EngineConfig
from loomwork.options import VersioningPolicy
from loomwork.store.sqlite import SQLiteStore
from loomwork.worker import WorkerOptions

BUILD_BLUE = "blue-a1b2c3d"
BUILD_BLUE_HOTFIX = "blue-a1b2c3d-hotfix"
BUILD_GREEN = "green-9f8e7d6"


async def main() -> None:
    clock = ManualClock()
    config = EngineConfig(
        poll_interval=DEMO_POLL_INTERVAL,
        workflow_versioning_policy={WORKFLOW_NAME: VersioningPolicy.PINNED},
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "blue_green.db"
        async with SQLiteStore(db_path) as store:
            print(rule("Blue takes two orders; each pins to the blue build on its first tick"))
            blue = lw.Loomwork(
                store=store,
                workflows=[process_widget_v1],
                steps=[steps.reserve_widget, steps.ship_widget],
                clock=clock,
                config=config,
                worker_options=WorkerOptions(
                    build_id=BUILD_BLUE,
                    # A short heartbeat interval (and so a short presence TTL, three
                    # intervals -- see WorkerOptions.build_heartbeat_interval) is what
                    # lets blue's presence visibly lapse a little later, once its
                    # workers stop heartbeating; a real deployment leaves the
                    # (real-time) default alone.
                    build_heartbeat_interval=30,
                    shutdown_grace=0.2,
                ),
            )
            async with blue:
                widget_1 = await blue.start_workflow(process_widget_v1, "widget-1", id="widget-1")
                widget_2 = await blue.start_workflow(process_widget_v1, "widget-2", id="widget-2")
                await wait_for_query(
                    clock, widget_1, "status", lambda s: s == "reserved", step=timedelta(0)
                )
                await wait_for_query(
                    clock, widget_2, "status", lambda s: s == "reserved", step=timedelta(0)
                )
                for summary in await blue.client.versioning.builds_summary():
                    print(f"  {summary}")
            # blue's worker (and its build-presence heartbeat) is now stopped, but
            # widget-1/2 are still pinned to it and still mid-flight: nothing has run
            # their next tick yet, and nothing will until a worker for BUILD_BLUE (or
            # one declared compatible with it) shows up again.

            print(rule("A bug is found in ship_widget; blue is retired before its orders finish"))
            await clock.advance(timedelta(seconds=100))  # long enough to lapse blue's presence
            async with lw.Loomwork(store=store, config=config, run_worker=False) as readonly:
                summaries = {
                    b.build_id: b for b in await readonly.client.versioning.builds_summary()
                }
                blue_status = summaries[BUILD_BLUE]
                print(f"  {blue_status}")
                assert blue_status.pinned_running_runs == 2
                assert blue_status.live_workers == 0
                assert blue_status.orphaned  # nobody can serve these two runs right now

            print(rule("Green (v2) comes up and immediately takes two new orders"))
            green = lw.Loomwork(
                store=store,
                workflows=[process_widget_v2],
                steps=[steps.reserve_widget_v2, steps.ship_widget_fixed],
                clock=clock,
                config=config,
                worker_options=WorkerOptions(
                    build_id=BUILD_GREEN, build_heartbeat_interval=3600, shutdown_grace=0.2
                ),
            )
            async with green:
                # Started, and pinned to green, before the hotfix build (below) ever
                # joins: nothing else is competing for these two runs' first tick, so
                # -- unlike the "normally" in docs/operations/deployment.md's rolling
                # deploy walkthrough -- which build claims it is not just probable but
                # guaranteed, which is what this demo needs to be deterministic.
                widget_3 = await green.start_workflow(process_widget_v2, "widget-3", id="widget-3")
                widget_4 = await green.start_workflow(process_widget_v2, "widget-4", id="widget-4")
                await wait_for_query(
                    clock, widget_3, "status", lambda s: s == "reserved", step=timedelta(0)
                )
                await wait_for_query(
                    clock, widget_4, "status", lambda s: s == "reserved", step=timedelta(0)
                )
                for summary in await green.client.versioning.builds_summary():
                    print(f"  {summary}")

                print(rule("A same-day hotfix of blue (v1, unchanged) joins to drain the rest"))
                hotfix = lw.Loomwork(
                    store=store,
                    workflows=[process_widget_v1],  # the hotfix changes no workflow code
                    steps=[steps.reserve_widget, steps.ship_widget_fixed],  # only the step body
                    clock=clock,
                    config=config,
                    worker_options=WorkerOptions(
                        build_id=BUILD_BLUE_HOTFIX,
                        build_heartbeat_interval=3600,
                        shutdown_grace=0.2,
                    ),
                )
                async with hotfix:
                    await hotfix.client.versioning.declare_compatible(BUILD_BLUE_HOTFIX, BUILD_BLUE)
                    summaries = {
                        b.build_id: b for b in await green.client.versioning.builds_summary()
                    }
                    print(f"after declaring {BUILD_BLUE_HOTFIX} compatible with {BUILD_BLUE}:")
                    print(f"  {summaries[BUILD_BLUE]}")
                    assert not summaries[BUILD_BLUE].orphaned  # the hotfix can serve them now

                    await clock.advance(PICK_WINDOW + timedelta(minutes=1))

                    handles = {
                        "widget-1": widget_1,
                        "widget-2": widget_2,
                        "widget-3": widget_3,
                        "widget-4": widget_4,
                    }
                    results = await asyncio.gather(*(h.result(timeout=5) for h in handles.values()))
                    for order_id, result in zip(handles, results, strict=True):
                        print(f"  {order_id}: {result!r}")

                    print(rule("Blue has fully drained and can be retired"))
                    final = {b.build_id: b for b in await green.client.versioning.builds_summary()}
                    # Zero pinned runs and no live worker: blue no longer shows up at
                    # all -- there is nothing left to report.
                    print(f"  {BUILD_BLUE}: {final.get(BUILD_BLUE)}")
                    assert BUILD_BLUE not in final
                    print(f"  {final[BUILD_GREEN]}")

    print(rule("What each build actually shipped"))
    print(f"reservations: {steps.reservations}")
    print(f"shipments: {steps.shipments}")


if __name__ == "__main__":
    asyncio.run(main())
