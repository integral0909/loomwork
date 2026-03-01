"""The workflows whose recorded histories are the golden corpus.

Changing what any of these workflows *does* invalidates its golden files, exactly as it
would invalidate the histories of real runs; that is the point. Change them only
together with a deliberate re-record (see ``record.py``), and never to make a failing
replay test pass.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from typing import Any

import loomwork as lw
from loomwork.sdk.definitions import StepDefinition, WorkflowDefinition

go = lw.Channel("go", str)
poke = lw.Channel("poke", str)


# --------------------------------------------------------------------------- steps


@lw.step(name="golden.lookup")
async def lookup(key: str) -> str:
    return key.upper()


@lw.step(name="golden.reject", retry=lw.RetryPolicy(max_attempts=1))
async def reject(key: str) -> str:
    raise lw.ApplicationError(f"{key} is not allowed", type="Rejected", non_retryable=True)


@lw.step(name="golden.slow")
async def slow() -> str:
    await asyncio.sleep(0.2)
    return "slow"


# --------------------------------------------------------------------------- workflows


@lw.workflow(name="golden.order")
async def order(order_id: str) -> dict[str, object]:
    """Steps (completed and failed), a durable timer, a patch, search attributes, a
    signal and an update."""
    approved: list[str] = []

    @lw.update("approve")
    def approve(by: str) -> int:
        approved.append(by)
        return len(approved)

    item = await lookup(order_id)
    with contextlib.suppress(lw.StepError):
        await reject(order_id)
    await lw.sleep(timedelta(hours=1))
    if lw.patched("golden-express"):
        item = f"{item}!"
    lw.upsert_search_attributes(golden_stage="waiting")
    note = await go.receive()
    await lw.wait_condition(lambda: bool(approved))
    return {"item": item, "note": note, "approved": approved}


@lw.workflow(name="golden.race")
async def race() -> str:
    """A timer cancelled by a signal, and a step cancelled by the workflow."""
    try:
        async with asyncio.timeout(3600):
            first = await poke.receive()
    except TimeoutError:
        first = "timeout"
    task = asyncio.ensure_future(slow())
    await lw.sleep(timedelta(milliseconds=1))
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return f"{first}, step cancelled"
    return f"{first}, step finished first"


@lw.workflow(name="golden.child")
async def child(n: int) -> int:
    if n < 0:
        raise lw.ApplicationError("negative", type="Negative")
    return n * 10


@lw.workflow(name="golden.parent")
async def parent(round_: int) -> list[object]:
    """Children that complete and fail, a signal to another workflow, and
    continue-as-new."""
    first = await lw.start_child(child, 4, id=f"golden-child-{round_}")
    results: list[object] = [first]
    try:
        await lw.start_child(child, -1, id=f"golden-bad-child-{round_}")
    except lw.ChildWorkflowError as exc:
        results.append(str(exc.type))
    go.send_to("golden-listener", f"from parent {round_}")
    if round_ == 0:
        lw.continue_as_new(1)
    return results


@lw.workflow(name="golden.listener")
async def listener() -> str:
    return await go.receive()


@lw.workflow(name="golden.waiter")
async def waiter() -> str:
    """Waits for instructions; used for failed, cancelled, terminated and timed-out
    closes."""
    instruction = await go.receive()
    if instruction == "fail":
        raise lw.ApplicationError("asked to fail", type="Asked")
    return instruction


WORKFLOWS: list[WorkflowDefinition[..., Any]] = [order, race, child, parent, listener, waiter]
STEPS: list[StepDefinition[..., Any]] = [lookup, reject, slow]
