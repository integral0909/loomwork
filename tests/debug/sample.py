"""A small workflow used across the replay-debugger tests: a step, a query, a wait."""

from __future__ import annotations

import loomwork as lw

decisions = lw.Channel("decisions", str)


@lw.step(name="dbg.step")
async def shout(text: str) -> str:
    return text.upper()


@lw.workflow(name="dbg.approval")
async def approval(item: str) -> str:
    loud = await shout(item)

    @lw.query("state")
    def state() -> dict[str, object]:
        return {"item": loud}

    decision = await decisions.receive()
    return f"{loud}: {decision}"


WORKFLOWS = [approval]
STEPS = [shout]
