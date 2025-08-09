"""Tests for ``examples.bulk_import``."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from examples.bulk_import import steps
from examples.bulk_import.models import Page
from examples.bulk_import.workflows import (
    PAGE_SIZE,
    PAGES_PER_RUN,
    bulk_import,
    import_page,
)
from loomwork import ExecutionStatus

if TYPE_CHECKING:
    from tests.conftest import AppFactory

WORKFLOWS = [bulk_import, import_page]
STEP_DEFS = [steps.import_records]


@pytest.fixture(autouse=True)
def _reset_sink() -> None:
    steps.sink.written.clear()


async def test_small_dataset_completes_in_a_single_run(make_app: AppFactory) -> None:
    app = await make_app(WORKFLOWS, STEP_DEFS, use_clock=False)
    total = PAGE_SIZE * 2  # well under PAGES_PER_RUN pages
    summary = await app.execute(bulk_import, 0, total, id="small-import", timeout=10)

    assert summary.runs == 1
    assert summary.pages_processed == 2
    assert summary.imported == total
    assert steps.sink.written == list(range(total))


async def test_large_dataset_continues_as_new_and_resumes_the_flaky_page(
    make_app: AppFactory,
) -> None:
    app = await make_app(WORKFLOWS, STEP_DEFS, use_clock=False)
    total = PAGE_SIZE * (PAGES_PER_RUN + 4)  # forces at least one continue-as-new
    handle = await app.start_workflow(bulk_import, 0, total, id="large-import")
    summary = await handle.result(timeout=20)

    assert summary.runs >= 2
    assert summary.imported == total
    # Every record was written exactly once, in order within each page, despite the
    # flaky page's first attempt failing partway through.
    assert steps.sink.written == list(range(total))

    runs = await app.store.list_executions(workflow_name="bulk_import.bulk_import")
    statuses = [r.status for r in runs]
    assert statuses.count(ExecutionStatus.COMPLETED) == 1
    assert statuses.count(ExecutionStatus.CONTINUED_AS_NEW) == len(runs) - 1
    for run in runs:
        # Each run's own history stays small, regardless of the dataset's total size.
        history = await app.store.load_history(run.run_id)
        assert len(history) < 60


async def test_flaky_page_retry_resumes_from_its_heartbeat_checkpoint(
    make_app: AppFactory,
) -> None:
    app = await make_app([import_page], STEP_DEFS, use_clock=False)
    page_two_start = steps.FLAKY_PAGE * PAGE_SIZE
    record_ids = list(range(page_two_start, page_two_start + PAGE_SIZE))

    result = await app.execute(
        import_page, Page(page_id=steps.FLAKY_PAGE, record_ids=record_ids), timeout=10
    )

    assert result.imported == PAGE_SIZE
    # The first attempt wrote half the page before failing; the retry must not repeat
    # those records.
    assert steps.sink.written == record_ids
    assert len(steps.sink.written) == len(set(steps.sink.written))
