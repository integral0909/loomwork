"""Paged bulk import: child workflows for fan-out/fan-in, ``continue_as_new`` to keep
the parent's own history bounded across a dataset far larger than one run should hold.
"""

from __future__ import annotations

import asyncio
import math

import loomwork as lw
from examples.bulk_import.models import ImportSummary, Page, PageResult
from examples.bulk_import.steps import import_records

#: Records per page.
PAGE_SIZE = 25
#: Child workflows started at once (bounded fan-out); the workflow waits for one batch
#: to finish before starting the next rather than starting every page simultaneously.
CONCURRENCY = 3
#: Pages processed per run before handing over to a fresh run via ``continue_as_new``.
PAGES_PER_RUN = 6


def _page(page_id: int, total_records: int) -> Page:
    start = page_id * PAGE_SIZE
    end = min(start + PAGE_SIZE, total_records)
    return Page(page_id=page_id, record_ids=list(range(start, end)))


@lw.workflow(name="bulk_import.import_page")
async def import_page(page: Page) -> PageResult:
    """Import one page. A child workflow (rather than a bare step) so each page gets
    its own history and identity -- useful once a page's import involves more than one
    step, and is what lets the parent fan out many pages concurrently and await them
    as a group."""
    imported = await import_records(page.page_id, page.record_ids)
    return PageResult(page_id=page.page_id, imported=imported)


@lw.workflow(name="bulk_import.bulk_import")
async def bulk_import(
    cursor: int, total_records: int, imported_so_far: int = 0, run_number: int = 1
) -> ImportSummary:
    """Import ``total_records`` records, ``PAGE_SIZE`` at a time.

    ``cursor`` is the index of the next page to process; ``imported_so_far`` and
    ``run_number`` carry accumulated progress across the ``continue_as_new`` chain, so
    the summary returned by the *last* run in the chain still reflects everything every
    run imported.
    """
    total_pages = math.ceil(total_records / PAGE_SIZE)
    imported_this_run = 0
    pages_this_run = 0

    while cursor < total_pages and pages_this_run < PAGES_PER_RUN:
        batch_size = min(CONCURRENCY, total_pages - cursor, PAGES_PER_RUN - pages_this_run)
        batch = [_page(cursor + offset, total_records) for offset in range(batch_size)]
        handles = [lw.start_child(import_page, page) for page in batch]
        results = await asyncio.gather(*handles)
        imported_this_run += sum(result.imported for result in results)
        cursor += batch_size
        pages_this_run += batch_size

    total_imported = imported_so_far + imported_this_run
    if cursor < total_pages:
        lw.continue_as_new(cursor, total_records, total_imported, run_number + 1)
    return ImportSummary(
        total_records=total_records,
        pages_processed=cursor,
        imported=total_imported,
        runs=run_number,
    )
