"""Two incompatible builds of the same pinned workflow.

Both versions are registered under the same workflow name,
``blue_green_deploy.process_widget``. Real deployments run one build's code at a time;
here both live in the same module so ``main.py`` can hand one to each of several
:class:`~loomwork.Loomwork` apps sharing one store, exactly as the "blue" and "green"
fleets of a real rolling deploy would each run their own build's code against the same
database.

v2 replaces the reservation step entirely -- a rewrite too large for
``loomwork.patched()`` to express cleanly in a small example (a real one is usually much
bigger: a library upgrade that changes step behaviour, or a workflow rewritten from
scratch). ``docs/adr/0006-worker-build-versioning.md`` calls exactly this out as the
case worker build pinning is for, rather than a patch.
"""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw
from examples.blue_green_deploy.steps import reserve_widget, reserve_widget_v2, ship_widget

WORKFLOW_NAME = "blue_green_deploy.process_widget"

#: How long a widget order sits reserved before it ships -- long enough that a demo (or
#: a test) can reliably catch a run mid-flight, already pinned to whichever build ran
#: its first tick.
PICK_WINDOW = timedelta(hours=1)


@lw.workflow(name=WORKFLOW_NAME)
async def process_widget_v1(order_id: str) -> str:
    stage = "reserving"

    @lw.query("status")
    def status() -> str:
        return stage

    reservation_id = await reserve_widget(order_id)
    stage = "reserved"
    await lw.sleep(PICK_WINDOW)
    tracking_id = await ship_widget(order_id, reservation_id)
    stage = "shipped"
    return tracking_id


@lw.workflow(name=WORKFLOW_NAME)
async def process_widget_v2(order_id: str) -> str:
    stage = "reserving"

    @lw.query("status")
    def status() -> str:
        return stage

    # Calls a *different* step at the same position v1 called ``reserve_widget`` --
    # unsafe for any run whose history already recorded v1's decision (see the
    # compatibility table in docs/guides/versioning.md: "calling a different step at
    # the same point" is never safe without a patch or, as here, a pinned build).
    reservation_id = await reserve_widget_v2(order_id)
    stage = "reserved"
    await lw.sleep(PICK_WINDOW)
    tracking_id = await ship_widget(order_id, reservation_id)
    stage = "shipped"
    return tracking_id
