"""Import a synthetic dataset larger than one run's history should hold.

.. code-block:: console

    uv run python -m examples.bulk_import.main

See ``README.md`` for what this demonstrates.
"""

from __future__ import annotations

import asyncio

import loomwork as lw
from examples.bulk_import.steps import import_records, sink
from examples.bulk_import.workflows import PAGE_SIZE, PAGES_PER_RUN, bulk_import, import_page
from examples.support import DEMO_POLL_INTERVAL, rule
from loomwork import MemoryStore
from loomwork.engine.config import EngineConfig
from loomwork.worker import WorkerOptions

TOTAL_RECORDS = 250  # ten pages of 25 -- more than PAGES_PER_RUN, so this needs two runs


async def main() -> None:
    store = MemoryStore()
    app = lw.Loomwork(
        store=store,
        workflows=[bulk_import, import_page],
        steps=[import_records],
        config=EngineConfig(poll_interval=DEMO_POLL_INTERVAL),
        worker_options=WorkerOptions(shutdown_grace=0.2),
    )
    async with app:
        print(
            rule(
                f"Importing {TOTAL_RECORDS} records, {PAGE_SIZE} per page, "
                f"{PAGES_PER_RUN} pages per run before continuing as new"
            )
        )
        handle = await app.start_workflow(bulk_import, 0, TOTAL_RECORDS, id="import-2024-01")
        summary = await handle.result(timeout=20)
        print(f"summary: {summary.model_dump()}")

    print(rule("How the run chain kept history bounded"))
    runs = await store.list_executions(workflow_name="bulk_import.bulk_import")
    runs.sort(key=lambda r: r.created_at)
    for run in runs:
        history = await store.load_history(run.run_id)
        print(f"  run {run.run_id}: status={run.status.value}, {len(history)} events")
    print(f"total pages imported across every run: {summary.pages_processed}")
    print(f"total records written to the sink: {len(sink.written)}")
    # A page's first attempt failed partway through (see steps.FLAKY_PAGE); the retry
    # resumed from its heartbeat checkpoint instead of re-writing what was already sent.
    assert len(sink.written) == len(set(sink.written)), "no record was written twice"


if __name__ == "__main__":
    asyncio.run(main())
