"""Garbage collection for blobs written by :class:`~loomwork.codec.offload.OffloadCodec`.

Offloaded blobs are content-addressed and never explicitly deleted by the codec (see its
module docstring): a blob becomes garbage only once nothing in any run's history
references its key any more, which happens when the run that produced it is deleted or
reset past the tick that referenced it. :func:`collect_garbage` finds and deletes those
blobs. It is a maintenance operation, not part of the engine's hot path -- run it
periodically out of band (a cron job, a scheduled workflow, an operator command), the same
way ``loomwork.archive`` is run out of band.

**Safety argument.** Call the moment a blob store ``put`` for key *K* durably completes
*t_put*, and the moment the history event referencing *K* is durably committed to the
store *t_commit*. Because :meth:`OffloadCodec.encode
<loomwork.codec.offload.OffloadCodec._encode_one>` always awaits the blob ``put`` before
returning the reference payload that becomes part of that event, ``t_put <= t_commit``
always -- but they are not the same transaction, so a scan that runs in the window between
them would see a blob that looks unreferenced even though it is about to be referenced.
``grace_period`` closes that window: a blob younger than ``now - grace_period`` is never
deleted, referenced or not. As long as ``grace_period`` comfortably exceeds the maximum
realistic ``t_commit - t_put`` (normally milliseconds to seconds; a tick that fails
between the two just retries and writes the same key again, since keys are content
hashes), every blob old enough to be considered is either genuinely unreferenced (safe to
delete) or already referenced in the now-committed history (found by the scan, and kept).
The default, 24 hours, is deliberately generous.

The scan itself streams: run ids come from the store a page at a time (keyset-paginated on
``(created_at, run_id)``, the same technique ``loomwork.archive`` and
``Client.batch_*`` use, so no run is skipped or repeated even if runs are being created or
closed concurrently), and each run's history is read a page at a time via ``after_id``.
Only the set of referenced blob keys accumulates for the whole scan; no history or blob
content is ever held in memory beyond one page.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from loomwork.blobs.base import ListableBlobStore
from loomwork.codec.offload import offload_key
from loomwork.codec.payload import Payload
from loomwork.codec.transform import transform_payloads
from loomwork.store.base import Store

DEFAULT_GRACE_PERIOD = timedelta(hours=24)

_EXECUTION_PAGE_SIZE = 200
_HISTORY_PAGE_SIZE = 1000


@dataclass(slots=True)
class GcReport:
    """What one :func:`collect_garbage` call did."""

    scanned_runs: int = 0
    referenced_blobs: int = 0
    scanned_blobs: int = 0
    deleted_blobs: int = 0
    deleted_bytes: int = 0
    skipped_within_grace_period: int = 0


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


async def _all_run_ids(store: Store, *, now: datetime, page_size: int) -> AsyncIterator[str]:
    """Every run id the store currently holds, any status, oldest ``created_at`` first.

    ``run_id != ''`` is a comparison the visibility query grammar accepts as an
    always-true filter (``run_id`` is never empty or NULL) -- there is no literal "match
    everything" query, so this is the standard way to page through every run with
    ``Store.query_executions`` (see ``Client._iter_matches`` and
    ``archive._select_closed_runs``, which use the same cursor technique restricted to a
    real filter).
    """
    from loomwork.visibility.schema import SearchAttributes

    schema = SearchAttributes({})
    cursor: tuple[datetime, str] | None = None
    while True:
        clauses = ["run_id != ''"]
        if cursor is not None:
            created_at, run_id = cursor
            ts = created_at.isoformat()
            clauses.append(
                f"(created_at > timestamp '{ts}' OR "
                f"(created_at = timestamp '{ts}' AND run_id > {_quote_literal(run_id)}))"
            )
        query = " AND ".join(clauses) + f" ORDER BY created_at ASC, run_id ASC LIMIT {page_size}"
        records = await store.query_executions(query, schema, now, limit=page_size)
        if not records:
            return
        for record in records:
            yield record.run_id
        if len(records) < page_size:
            return
        last = records[-1]
        cursor = (last.created_at, last.run_id)


async def _referenced_keys_in_run(store: Store, run_id: str, *, page_size: int) -> set[str]:
    keys: set[str] = set()

    async def _collect(payloads: Sequence[Payload]) -> list[Payload]:
        for payload in payloads:
            key = offload_key(payload)
            if key is not None:
                keys.add(key)
        return list(payloads)

    after_id = 0
    while True:
        events = await store.load_history(run_id, after_id=after_id, limit=page_size)
        if not events:
            return keys
        await transform_payloads(events, _collect)
        after_id = events[-1].event_id
        if len(events) < page_size:
            return keys


async def collect_garbage(
    blob_store: ListableBlobStore,
    store: Store,
    *,
    grace_period: timedelta = DEFAULT_GRACE_PERIOD,
    now: datetime | None = None,
    prefix: str = "",
    dry_run: bool = False,
) -> GcReport:
    """Delete blobs no run's history references any more.

    Args:
        blob_store: scanned with :meth:`~loomwork.blobs.base.ListableBlobStore.list` and
            pruned with ``delete``.
        store: scanned for every run's history (any status -- a still-running execution's
            in-progress history is just as much a reference as a closed one's).
        grace_period: blobs younger than this are never deleted; see the module
            docstring for why this is what makes deletion safe against a scan racing an
            in-flight offload.
        prefix: only consider blobs whose key starts with this (matches
            :class:`~loomwork.codec.offload.OffloadCodec`'s ``key_prefix``, and lets one
            blob store be shared with other, unrelated data this function must never
            touch).
        dry_run: report what would be deleted without deleting anything.
    """
    now = now if now is not None else datetime.now(UTC)
    cutoff = now - grace_period
    report = GcReport()

    referenced: set[str] = set()
    async for run_id in _all_run_ids(store, now=now, page_size=_EXECUTION_PAGE_SIZE):
        report.scanned_runs += 1
        referenced |= await _referenced_keys_in_run(store, run_id, page_size=_HISTORY_PAGE_SIZE)
    report.referenced_blobs = len(referenced)

    async for info in blob_store.list(prefix):
        report.scanned_blobs += 1
        if info.key in referenced:
            continue
        if info.last_modified > cutoff:
            report.skipped_within_grace_period += 1
            continue
        if not dry_run:
            await blob_store.delete(info.key)
        report.deleted_blobs += 1
        report.deleted_bytes += info.size
    return report
