"""Evolve a running workflow with ``loomwork.patched()``, and check the change against
recorded history with ``loomwork.versioning`` before ever deploying it.

.. code-block:: console

    uv run python -m examples.versioned_workflow.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import loomwork as lw
from examples.support import DEMO_POLL_INTERVAL, advance_until, rule
from examples.versioned_workflow.steps import send_email, send_sms, sent
from examples.versioned_workflow.workflows import onboard_v1, onboard_v2, onboard_v2_unsafe
from loomwork import ManualClock, MemoryStore
from loomwork.engine.config import EngineConfig
from loomwork.sdk.definitions import Registry
from loomwork.versioning import check_compatibility
from loomwork.worker import WorkerOptions

_STEPS = [send_email, send_sms]


async def main() -> None:
    clock = ManualClock()
    store = MemoryStore()
    config = EngineConfig(poll_interval=DEMO_POLL_INTERVAL)

    print(rule("Running under v1: user-1 sends a welcome email, then waits an hour"))
    v1_app = lw.Loomwork(
        store=store,
        workflows=[onboard_v1],
        steps=_STEPS,
        clock=clock,
        config=config,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    await v1_app.start()
    await v1_app.start_workflow(onboard_v1, "user-1", id="onboard-user-1")
    await advance_until(clock, lambda: store.timer_count == 1, step=timedelta(0))
    print(f"user-1 is now waiting; sent so far: {sent}")
    await v1_app.stop()  # the old worker process is retired before the new code deploys

    print(rule("Before deploying: checking two v2 candidates against recorded history"))
    safe_report = await check_compatibility(store, Registry([onboard_v2]))
    print(f"guarded with patched(): {safe_report.format_text()}")
    unsafe_report = await check_compatibility(store, Registry([onboard_v2_unsafe]))
    print(f"unguarded: {unsafe_report.format_text()}")
    assert safe_report.ok
    assert not unsafe_report.ok

    print(rule("Deploying the guarded v2: a new run takes SMS, user-1 still gets email"))
    v2_app = lw.Loomwork(
        store=store,
        workflows=[onboard_v2],
        steps=_STEPS,
        clock=clock,
        config=config,
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with v2_app:
        old_handle = v2_app.get_handle(workflow_id="onboard-user-1", result_type=str)
        new_handle = await v2_app.start_workflow(onboard_v2, "user-2", id="onboard-user-2")
        # Wait for user-2's first tick (which schedules its own hour-long durable sleep)
        # before jumping the clock, so that sleep is timed from "now", not from whatever
        # the clock happens to read once the tick eventually gets around to running.
        await advance_until(clock, lambda: store.timer_count == 2, step=timedelta(0))
        await clock.advance(timedelta(hours=1, seconds=1))
        old_result = await old_handle.result(timeout=5)
        new_result = await new_handle.result(timeout=5)
        print(f"user-1 (recorded under v1, replayed under v2): {old_result!r}")
        print(f"user-2 (started fresh under v2): {new_result!r}")

    print(rule("After the deploy: both closed runs are compatible with v2"))
    final_report = await check_compatibility(store, Registry([onboard_v2]))
    print(final_report.format_text())
    assert final_report.summary() == {"versioned_workflow.onboard": (2, 0)}

    print(rule("Every notification actually sent"))
    for channel, user_id in sent:
        print(f"  {channel}: {user_id}")


if __name__ == "__main__":
    asyncio.run(main())
