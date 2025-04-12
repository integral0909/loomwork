"""Search attributes set by workflows and found with visibility queries."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

import loomwork as lw
from loomwork import ManualClock, MemoryStore
from loomwork.visibility import AttributeType, SearchAttributes
from loomwork.worker import WorkerOptions
from tests.helpers import FAST_CONFIG, eventually

pytestmark = pytest.mark.integration

SCHEMA = SearchAttributes.define(
    customer_tier=AttributeType.STRING,
    order_total=AttributeType.INT,
    stage=AttributeType.STRING,
)


@lw.workflow(name="vis.order")
async def order(total: int) -> str:
    tier = lw.search_attributes().get("customer_tier", "standard")
    lw.upsert_search_attributes(order_total=total, stage="paying")
    await lw.sleep(timedelta(minutes=5))
    lw.upsert_search_attributes(stage="shipped")
    return f"{tier}:{total}"


async def test_queries_find_runs_by_attributes(clock: ManualClock) -> None:
    store = MemoryStore()
    app = lw.Loomwork(
        store=store,
        workflows=[order],
        clock=clock,
        config=replace(FAST_CONFIG, search_attributes=SCHEMA),
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with app:
        gold = await app.start_workflow(order, 500, search_attributes={"customer_tier": "gold"})
        await app.start_workflow(order, 20)
        await eventually(lambda: store.timer_count == 2)

        paying = await app.client.query_runs("attr.stage = 'paying' ORDER BY created_at ASC")
        assert len(paying) == 2
        big = await app.client.query_runs("attr.order_total >= 100")
        assert [r.run_id for r in big] == [gold.run_id]
        assert await gold.search_attributes() == {
            "customer_tier": "gold",
            "order_total": 500,
            "stage": "paying",
        }

        await clock.advance(timedelta(minutes=5))
        assert await gold.result(timeout=5) == "gold:500"
        await eventually(lambda: store.timer_count == 0)
        shipped = await app.client.query_runs(
            "attr.stage = 'shipped' AND status = 'completed' AND attr.customer_tier = 'gold'"
        )
        assert [r.run_id for r in shipped] == [gold.run_id]
