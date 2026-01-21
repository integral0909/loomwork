"""Steps for the blue/green worker-build demo.

``ship_widget`` (the original) and ``ship_widget_fixed`` (a same-day hotfix) are
registered under the *same* step name, ``blue_green_deploy.ship_widget``: workflow code
never says which implementation actually runs a step, only which name to schedule --
whichever process claims the step job looks that name up in its own registry (see
``docs/adr/0006-worker-build-versioning.md``'s "steps are not pinned by default").
Swapping a step's implementation like this is always safe, exactly like redeploying a
bugfix to a step's body in any other Loomwork application.

``reserve_widget`` and ``reserve_widget_v2`` are the opposite case: registered under
*different* names, standing in for a rewrite too large to express as a step swap or a
``loomwork.patched()`` branch (a new inventory API, say). Calling one where a run's
history already recorded the other is exactly the kind of change ``docs/guides/
versioning.md``'s compatibility table calls unsafe -- which is why ``main.py`` gives the
workflow that calls it a whole separate worker build, ``VersioningPolicy.PINNED``,
rather than a patch.
"""

from __future__ import annotations

import loomwork as lw

#: (build, reservation_id) and (order_id, tracking_id) pairs, in the order steps
#: actually ran -- a stand-in for the warehouse and carrier systems these steps would
#: really call.
reservations: list[tuple[str, str]] = []
shipments: list[tuple[str, str]] = []


@lw.step(name="blue_green_deploy.reserve_widget")
async def reserve_widget(order_id: str) -> str:
    """The original (v1) reservation flow."""
    reservation_id = f"res-{order_id}"
    reservations.append(("v1", reservation_id))
    return reservation_id


@lw.step(name="blue_green_deploy.reserve_widget_v2")
async def reserve_widget_v2(order_id: str) -> str:
    """A rewritten reservation flow against a new inventory API (v2)."""
    reservation_id = f"res2-{order_id}"
    reservations.append(("v2", reservation_id))
    return reservation_id


@lw.step(name="blue_green_deploy.ship_widget")
async def ship_widget(order_id: str, _reservation_id: str) -> str:
    """The original shipping step -- ships with a placeholder tracking number.

    A real deployment would have caught this in review; it stands in for the kind of
    small, contained bug a same-day hotfix (:func:`ship_widget_fixed`) exists to fix
    without waiting for every in-flight order to finish first.
    """
    tracking_id = f"track-TODO-{order_id}"
    shipments.append((order_id, tracking_id))
    return tracking_id


@lw.step(name="blue_green_deploy.ship_widget")
async def ship_widget_fixed(order_id: str, reservation_id: str) -> str:
    """The hotfixed shipping step: a real tracking number, from the reservation."""
    tracking_id = f"track-{reservation_id}"
    shipments.append((order_id, tracking_id))
    return tracking_id
