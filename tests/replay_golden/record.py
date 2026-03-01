"""Record the golden histories: ``python -m tests.replay_golden.record [--force]``.

Existing files are left alone unless ``--force`` is given: a golden history is a
contract with every run already recorded in production, so replacing one should be a
deliberate act reviewed in the diff, not a side effect of running a script.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import loomwork as lw
from loomwork.store.records import ExecutionStatus
from loomwork.testing import WorkflowEnvironment, record_golden
from tests.replay_golden import corpus

HISTORIES = Path(__file__).parent / "histories"

Scenario = Callable[[WorkflowEnvironment], Awaitable[dict[str, list[Any]]]]


async def _first_tick(handle: lw.WorkflowHandle[Any]) -> None:
    async with asyncio.timeout(10):
        while "tick_completed" not in [e.kind for e in await handle.history()]:  # noqa: ASYNC110
            await asyncio.sleep(0.005)


async def _closed(handle: lw.WorkflowHandle[Any]) -> None:
    async with asyncio.timeout(10):
        while not (await handle.describe()).is_closed:  # noqa: ASYNC110
            await asyncio.sleep(0.005)


async def order(env: WorkflowEnvironment) -> dict[str, list[Any]]:
    handle = await env.start_workflow(corpus.order, "o-1", id="golden-order")
    await _first_tick(handle)
    await handle.signal(corpus.go, "ship it")
    await handle.update("approve", "ada", timeout=10)
    await handle.result(timeout=10)
    return {"order": await handle.history()}


async def race(env: WorkflowEnvironment) -> dict[str, list[Any]]:
    # Manual time: the signal must beat the hour-long timeout, and the workflow must
    # cancel the step while it is still running.
    handle = await env.start_workflow(corpus.race, id="golden-race")
    await _first_tick(handle)
    await handle.signal(corpus.poke, "hi")
    async with asyncio.timeout(10):
        while "step_scheduled" not in [e.kind for e in await handle.history()]:  # noqa: ASYNC110
            await asyncio.sleep(0.005)
    await env.sleep(1)
    await handle.result(timeout=10)
    return {"race": await handle.history()}


async def parent(env: WorkflowEnvironment) -> dict[str, list[Any]]:
    listener = await env.start_workflow(corpus.listener, id="golden-listener")
    handle = await env.start_workflow(corpus.parent, 0, id="golden-parent")
    await handle.result(timeout=10)
    await listener.result(timeout=10)
    runs = sorted(await env.list_executions(workflow=corpus.parent), key=lambda r: r.created_at)
    histories = {
        f"parent-round-{i}": await env.store.load_history(run.run_id) for i, run in enumerate(runs)
    }
    histories["listener"] = await listener.history()
    return histories


async def closes(env: WorkflowEnvironment) -> dict[str, list[Any]]:
    failed = await env.start_workflow(corpus.waiter, id="golden-failed")
    canceled = await env.start_workflow(corpus.waiter, id="golden-canceled")
    terminated = await env.start_workflow(corpus.waiter, id="golden-terminated")
    timed_out = await env.client.start(corpus.waiter, id="golden-timed-out", run_timeout=60)
    for handle in (failed, canceled, terminated):
        await _first_tick(handle)
    await failed.signal(corpus.go, "fail")
    await canceled.cancel("no longer needed")
    await terminated.terminate("operator decision")
    for handle in (failed, canceled, terminated, timed_out):
        await _closed(handle)
    statuses = {(await h.describe()).status for h in (failed, canceled, terminated, timed_out)}
    assert statuses == {
        ExecutionStatus.FAILED,
        ExecutionStatus.CANCELED,
        ExecutionStatus.TERMINATED,
        ExecutionStatus.TIMED_OUT,
    }, statuses
    return {
        "waiter-failed": await failed.history(),
        "waiter-canceled": await canceled.history(),
        "waiter-terminated": await terminated.history(),
        "waiter-timed-out": await timed_out.history(),
    }


#: Scenarios and whether the environment skips idle time for them.
SCENARIOS: list[tuple[Scenario, bool]] = [
    (order, True),
    (race, False),
    (parent, True),
    (closes, True),
]


async def record_all(directory: Path = HISTORIES, *, force: bool = False) -> list[Path]:
    written = []
    for scenario, auto_time_skipping in SCENARIOS:
        async with WorkflowEnvironment(
            workflows=corpus.WORKFLOWS,
            steps=corpus.STEPS,
            auto_time_skipping=auto_time_skipping,
        ) as env:
            histories = await scenario(env)
        for name, events in histories.items():
            path = directory / f"{name}.json"
            if path.exists() and not force:
                continue
            written.append(await record_golden(events, path))
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="overwrite existing files")
    args = parser.parse_args()
    for path in asyncio.run(record_all(force=args.force)):
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
