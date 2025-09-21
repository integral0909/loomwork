# Archiving

`loomwork store prune` deletes closed runs so a store does not grow forever, but
compliance and later debugging often need them kept somewhere cheap. `loomwork archive`
writes closed runs to a directory of gzip-compressed JSON Lines segments plus a manifest,
independent of any store backend, and reads them back with the same streaming guarantees
a live store gives you: `archive list`, `archive show` and `loomwork replay --archive`
all work without ever loading more than one run into memory at a time.

## The format

An archive is a directory:

```
manifest.json
segment-00001.jsonl.gz
segment-00002.jsonl.gz
...
```

Each segment is gzip-compressed JSON Lines, one line per archived run: its execution
record, its search attributes, and its complete event history, in the same shape
`loomwork workflow history --export` produces (see `loomwork.history.serde`). Segments
rotate by size (`--segment-size`, default 64MB); the last run written always finishes
its segment, so a segment can exceed the limit slightly rather than split a run.

`manifest.json` is self-describing:

```json
{
  "format_version": 1,
  "created_at": "2024-06-01T09:00:00+00:00",
  "loomwork_version": "0.1.0",
  "source": "sqlite:///prod.db",
  "run_count": 15234,
  "time_range": {"start": "...", "end": "..."},
  "segments": [
    {"filename": "segment-00001.jsonl.gz", "run_count": 1000, "byte_size": 6291456,
     "sha256": "...", "first_closed_at": "...", "last_closed_at": "..."}
  ]
}
```

`source` is the store URL with its password redacted (or a server/namespace
description), never a raw connection string. Every segment carries its own SHA-256
checksum, run count and closed-run time range, so `loomwork archive verify` can check
each one independently and report precisely which segment (and which line within it) is
damaged.

**Payloads are archived exactly as the store handed them over: still encoded.** If your
application encrypts or compresses payloads (see [payload
encryption](../payload-encryption.md)), archiving never needs the keys, and reading an
archive back (`archive list`, `archive show` without `--export`) never needs them
either -- only replaying a run's workflow code against its history
(`loomwork replay`) does, exactly as it would against a live run.

## Creating an archive

```
loomwork archive create --older-than 30d --out ./archive-2024-06
```

This selects every *closed* run whose `closed_at` is older than the cutoff, streams
their histories out of the store in bounded batches (so archiving a store with millions
of old runs never holds more than one batch's histories in memory), and writes them into
segments. Narrow the selection with a [visibility query](../visibility-queries.md)
filter (no `ORDER BY` or `LIMIT` -- those are the archiver's own):

```
loomwork archive create --older-than 90d --out ./archive --query "attr.tier = 'gold'"
```

Add `--prune` to delete the archived runs from the store once the archive is confirmed
durable:

```
loomwork archive create --older-than 30d --out ./archive-2024-06 --prune
```

`--prune` is rejected together with `--query`: pruning always deletes *every* closed run
older than the cutoff, which is a superset of what a filtered archive selected, so
combining them could delete runs that were never archived. Archive a filtered subset,
then prune everything older than the same cutoff separately with `loomwork store prune`
once you are satisfied nothing you needed was left out.

### Crash safety of "archive then prune"

Deleting a run before it is safely archived would lose it forever; the ordering here is
built to make that impossible:

1. Every segment is written and **fsynced** as it is closed (data, not just the
   directory entry).
2. The manifest is written and fsynced **last**, only after every segment. A crash
   before this point leaves segment files without a manifest -- an incomplete archive
   that is never mistaken for a complete one, and safe to discard or retry. Nothing has
   been deleted from the store yet.
3. Before pruning, the archive is **re-read from disk and checksum-verified**
   independently of the in-memory write that just happened -- the same check
   `loomwork archive verify` performs. Only if that passes does pruning begin.
4. Pruning itself deletes exactly the runs matched by the same `closed_at < cutoff`
   condition used to select them for archiving. Since `closed_at` is set once and never
   changes, that set can only shrink between archiving and pruning, never grow: pruning
   can never delete a run the archive does not already contain.

A crash during step 1 or 2 leaves the store completely untouched. A crash during step 4
(mid-prune) leaves some already-archived runs still in the store; re-running `--prune` or
`loomwork store prune` later deletes them with no risk, because the archive already
durably contains them. Nothing is ever deleted before it is durably archived, and if a
write itself fails partway, every segment file it created during that attempt (closed or
not) is removed, so a failed attempt leaves either a complete archive or an empty
directory -- never a partial one for a later `verify()` to puzzle over.

## Reading an archive

```
loomwork archive verify ./archive-2024-06
loomwork archive list ./archive-2024-06 [--workflow NAME]
loomwork archive show ./archive-2024-06 WORKFLOW_ID [--run RUN_ID] [--export FILE]
```

`verify` recomputes every segment's checksum and re-parses every line, so it takes as
long as reading the whole archive; run it after copying an archive to new storage, or
before pruning from a script of your own. `list` and `show` stream the archive instead:
memory use stays constant no matter how many runs it holds. `show --export FILE` writes
a run's history as the same portable JSON document `workflow history --export` produces,
ready for `loomwork compat --history` or `loomwork replay --history`.

## Programmatic API

```python
from loomwork.archive import ArchiveWriter, ArchiveReader, archive_and_prune

manifest = await ArchiveWriter("./archive").write(
    store, cutoff=cutoff, source=redact_url(store_url),
)

reader = ArchiveReader("./archive")
reader.verify()
for run in reader.iter_runs(workflow_name="orders.process"):
    ...  # run.record, run.search_attributes, run.history

result = await archive_and_prune(store, "./archive", cutoff=cutoff, source=..., prune=True)
```

See `loomwork.archive` for the full API: `ArchiveWriter`, `ArchiveReader`,
`ArchivedRun`, `ArchiveManifest`, `archive_and_prune`, and the `ArchiveError` family
(`ArchiveNotFoundError`, `ArchiveCorruptionError`) raised on lookup misses and damaged
archives.
