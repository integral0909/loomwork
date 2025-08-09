"""Steps for the bulk import example: writing records to a downstream data sink."""

from __future__ import annotations

import loomwork as lw

#: The one page whose first attempt fails partway through, to demonstrate resuming
#: from a heartbeat checkpoint instead of re-importing records already written.
FLAKY_PAGE = 2


class DataSink:
    """Stands in for the system records are imported into (a database, a search
    index, ...). Records the order records actually arrived in, so a test can confirm
    a resumed attempt didn't re-write records the failed attempt already wrote."""

    def __init__(self) -> None:
        self.written: list[int] = []

    def write(self, record_id: int) -> None:
        self.written.append(record_id)


sink = DataSink()


@lw.step(
    name="bulk_import.import_records",
    retry=lw.RetryPolicy(initial_interval=0.01, max_interval=0.05, max_attempts=3, jitter=0),
    heartbeat_timeout=5,
)
async def import_records(page_id: int, record_ids: list[int]) -> int:
    """Write ``record_ids`` to the sink one at a time, heartbeating progress.

    ``loomwork.heartbeat_details()`` returns how many records the *previous* attempt
    of this same step already wrote, so a retry resumes instead of starting the page
    over -- important once a page is large enough that redoing it from scratch would be
    wasteful, and essential if writing a record is not itself idempotent.
    """
    start = lw.heartbeat_details() or 0
    for index in range(start, len(record_ids)):
        sink.write(record_ids[index])
        await lw.heartbeat(index + 1)
        if page_id == FLAKY_PAGE and index == len(record_ids) // 2 and lw.step_info().attempt == 1:
            raise ConnectionError("data sink connection reset mid-page")
    return len(record_ids)
