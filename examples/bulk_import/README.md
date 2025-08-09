# Bulk import

Importing a dataset far larger than any single workflow run should hold in memory (or
in its recorded history): paged fan-out/fan-in with bounded concurrency, and
`continue_as_new` to keep each run's own history small no matter how much data there
is in total.

## Scenario

`bulk_import` (in `workflows.py`) imports `total_records` records, `PAGE_SIZE` at a
time. Each page is processed by its own **child workflow**, `import_page`, so that a
page with more than one step of its own would still get its own bounded history; the
parent starts up to `CONCURRENCY` of them at once and waits for that batch with
`asyncio.gather` before starting the next -- bounded fan-out/fan-in, not "start every
page at once":

```python
batch = [_page(cursor + offset, total_records) for offset in range(batch_size)]
handles = [lw.start_child(import_page, page) for page in batch]
results = await asyncio.gather(*handles)
```

After `PAGES_PER_RUN` pages, if the dataset isn't finished, the workflow calls
`loomwork.continue_as_new`, passing its accumulated progress (`cursor`,
`imported_so_far`, `run_number`) as the arguments to a fresh run under the same
workflow id. `WorkflowHandle.result()` follows the chain automatically, so a caller
sees one logical import regardless of how many runs it took.

Within a page, `import_records` (in `steps.py`) writes records to a (simulated) data
sink one at a time, calling `loomwork.heartbeat(index + 1)` after each one. One page is
rigged to fail partway through its first attempt; because the retry reads
`loomwork.heartbeat_details()` to find out how many records the previous attempt
already wrote, it resumes from there instead of re-writing them -- important both for
efficiency and because writing a record here is not assumed to be idempotent.

## Loomwork features used

- Child workflows (`loomwork.start_child`) for fan-out, awaited together with
  `asyncio.gather` for fan-in, in batches bounded by `CONCURRENCY`.
- `loomwork.continue_as_new` to keep a long-lived workflow's history bounded, carrying
  progress forward as arguments to the next run.
- `loomwork.heartbeat` / `loomwork.heartbeat_details` for a resumable checkpoint inside
  a step that does meaningful work in a loop.
- `WorkflowHandle.result()` transparently following a `continue_as_new` chain.

## Running it

```console
uv run python -m examples.bulk_import.main
```

250 records at 25 per page is 10 pages; at 6 pages per run that takes two runs, which
the demo shows by printing each run's id, status and history length. Nothing here
models multi-day waits, so the demo runs against the real clock -- it finishes in well
under a second because there is simply not much work to do.

## Tests

`tests/examples/test_bulk_import.py` checks:

- the full import completes with every record written exactly once, across more than
  one run (asserting on `store.list_executions` that a `continued_as_new` run precedes
  the final `completed` one);
- the page whose first attempt fails partway through resumes from its heartbeat
  checkpoint instead of re-writing already-imported records.
