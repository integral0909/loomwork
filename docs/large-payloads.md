# Large payloads (claim check)

Workflow histories are replayed in full, kept for the life of a run, and often archived,
so they are meant to stay small. Some steps don't cooperate: a report, an image, a
dataset a step downloads and hands to the next step can easily be megabytes. `OffloadCodec`
is a `PayloadCodec` (see `docs/payload-encryption.md` and `internals.md`'s "Payloads and
codecs" section for how the codec chain fits into the engine) that moves payloads above a
size threshold out of the history and into a blob store, leaving a small reference behind.
Workflow and step code never see the difference -- they still receive the full value; only
what reaches the store changes.

## When to use it

Add `OffloadCodec` to your codec chain when steps routinely produce or consume values
that are large enough to matter for history size or storage cost -- typically from tens of
kilobytes up. It costs nothing for payloads below the threshold (one length check), so
there is little downside to enabling it even if only a few workflows actually produce
large values.

It does not help with values so large they shouldn't be workflow data at all: a step that
processes gigabytes should read from and write to a stable location (your own object
storage, a database) and pass a reference through the workflow, not the bytes themselves.
`OffloadCodec` is for the megabyte range, not the gigabyte range -- everything still passes
through the engine's normal argument/result path, one call at a time.

## Configuration

`OffloadCodec` needs a `loomwork.blobs.BlobStore` to write to:

```python
from loomwork.blobs import FileSystemBlobStore
from loomwork.codec import CodecChain, DataConverter, PayloadSizeLimitCodec
from loomwork.codec.offload import OffloadCodec

blob_store = FileSystemBlobStore("/var/lib/loomwork/blobs")
chain = CodecChain([
    OffloadCodec(blob_store, threshold_bytes=128 * 1024),
    PayloadSizeLimitCodec(max_bytes=2 * 1024 * 1024),
])
converter = DataConverter(codec=chain)
```

- `threshold_bytes` (default 128 KiB): payloads at or above this size are offloaded;
  smaller ones pass through untouched.
- `key_prefix` (default `""`): prepended to every blob key. Useful for sharing a bucket
  or root directory with other data, and for scoping a `collect_garbage` scan (below) to
  just this codec's blobs with its `prefix` argument.

### Ordering in the codec chain

**`OffloadCodec` must be the outermost codec: last in the list on encode, so it runs
after compression and encryption.** `CodecChain` applies codecs in order on encode and in
reverse order on decode, so putting it last means it is also the *first* to run on
decode -- it fetches and reconstitutes the blob before anything else tries to decompress
or decrypt it. Concretely:

```python
from loomwork.codec import CompressionCodec, EncryptionCodec

chain = CodecChain([
    CompressionCodec(),
    EncryptionCodec(keyring),
    OffloadCodec(blob_store, threshold_bytes=128 * 1024),
    PayloadSizeLimitCodec(max_bytes=2 * 1024 * 1024),
])
```

This ordering is what makes the blob store see only ciphertext when encryption is
configured -- it never sees plaintext, exactly like the primary store (see "Security
properties" below). It also means `PayloadSizeLimitCodec`, if used, should run *after*
`OffloadCodec`: the limit then applies to what actually reaches the store's history (the
small reference, typically well under a kilobyte) rather than the pre-offload value, so a
payload that would have exceeded the limit is offloaded instead of rejected.

### Blob stores

`loomwork.blobs` ships three implementations of the same small `BlobStore` protocol
(`put`, `get`, `delete`, `exists`, and `list` for stores that support enumeration):

- **`MemoryBlobStore`** -- process-local, for tests.
- **`FileSystemBlobStore`** -- a content-addressed directory tree on disk. Keys are
  hashed before touching the filesystem (never used as path components directly), so a
  key cannot escape the configured root regardless of what it contains; writes are
  atomic (temp file, fsync, rename, directory fsync), so a crash mid-write never leaves a
  corrupt blob in place, only the old content or nothing.
- **`S3BlobStore`** -- S3 or an S3-compatible endpoint (MinIO, Cloudflare R2, ...), signed
  with a hand-written AWS Signature Version 4 implementation (no `boto3`). Needs the
  `s3` extra: `pip install "loomwork[s3]"`.

`blob_store_from_url` builds any of them from a connection string, which is usually more
convenient for configuration:

```python
from loomwork.blobs import blob_store_from_url

blob_store_from_url("memory://")
blob_store_from_url("file:///var/lib/loomwork/blobs")
blob_store_from_url("s3://my-bucket/loomwork-blobs?region=us-west-2")
blob_store_from_url("s3://my-bucket?endpoint=http://localhost:9000&path_style=true")  # MinIO
```

#### S3 setup

`S3BlobStore` credentials always come from the standard AWS environment variables
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) -- never from the URL
or from an argument, so they cannot end up logged alongside a connection string. Pass
`credentials=S3Credentials(...)` explicitly only if your deployment does not already
inject them as environment variables.

`s3://` URL query parameters:

| Parameter | Meaning |
| --- | --- |
| `region` | AWS region, or whatever region string your S3-compatible endpoint expects (default `us-east-1`) |
| `endpoint` | set for anything that is not AWS S3 itself |
| `path_style` | `true` for `endpoint/bucket/key` addressing instead of `bucket.endpoint/key`; defaults to `true` whenever `endpoint` is set, since most self-hosted endpoints lack the wildcard DNS virtual-hosted style needs |
| `sse` | sets `x-amz-server-side-encryption` on every write, for example `sse=AES256` or `sse=aws:kms` |

Requests that come back with a 5xx status or a throttling response are retried with
exponential backoff (`max_retries`, `backoff_base` on `S3BlobStore`); a 404 on `get` is
raised as `loomwork.blobs.BlobNotFoundError`, not a generic HTTP error.

## Security properties (with encryption)

With the ordering above, `OffloadCodec` never weakens `EncryptionCodec`'s guarantees (see
`payload-encryption.md`'s threat model): the blob store receives exactly the ciphertext
`EncryptionCodec` produced, under a key derived from *that ciphertext's* SHA-256 hash.
Anyone with read access to the blob store -- object storage logs, a filesystem backup, an
S3 bucket policy misconfiguration -- sees the same ciphertext a database administrator
would see in the primary store, nothing more.

**Integrity.** The blob key doubles as an integrity check: it is `sha256` of the exact
bytes written, verified again on every read. A blob whose bytes were altered after being
written (bit rot, a corrupted backup, tampering if some other integrity protection was
bypassed) is caught on decode and raises `loomwork.codec.errors.IntegrityError` -- the
same exception `EncryptionCodec` raises for a tampered ciphertext, so callers do not need
to distinguish the two failure modes.

**Deduplication.** Because the key is content-addressed, two payloads with identical
bytes (a retried tick re-encoding the same result is the common case) always write the
same key. This is safe under concurrency without any coordination between writers: two
concurrent `put` calls for the same key are, by construction, writing identical content,
so whichever finishes last still leaves the store in the same state either would have on
its own.

**Missing blobs.** If a referenced blob is gone, decode raises `CodecError` naming the
key (not a bare `BlobNotFoundError` with no context), which points straight at what to
look for: a retention policy or `collect_garbage` run that deleted it before every run
referencing it was gone (see below), or operator error.

## Garbage collection

`OffloadCodec` never deletes a blob -- content-addressed keys have no natural point at
which the codec itself knows nothing references them any more (a run's history is append-only
and other runs may dedupe onto the same key). `loomwork.blobs.collect_garbage` does this
out of band:

```python
from datetime import timedelta

from loomwork.blobs import collect_garbage

report = await collect_garbage(blob_store, store, grace_period=timedelta(hours=24))
print(report)  # GcReport(scanned_runs=..., referenced_blobs=..., deleted_blobs=..., ...)
```

It streams: run ids are read from `store` a page at a time (a keyset cursor over
`(created_at, run_id)`, so a scan is never skipped or repeated by concurrent activity),
and each run's history a page at a time, so scanning a store with millions of runs never
holds more than one page of history in memory. Only the set of referenced blob keys
accumulates for the whole scan.

**Safety argument.** `OffloadCodec.encode` always finishes writing a blob (awaits `put`)
before returning the small reference payload that becomes part of a history event, but the
blob write and the event's commit to the store are not the same transaction -- there is a
window, normally milliseconds, where a blob exists but nothing committed yet references
it. `grace_period` (default 24 hours, deliberately generous) is what makes scanning safe
across that window: a blob younger than `now - grace_period` is *never* deleted, whether
the scan currently sees it as referenced or not. By the time a blob is old enough to be
considered, either its referencing event has long since committed (so the scan finds it)
or the write was abandoned and it is genuinely orphaned. Run `collect_garbage`
periodically out of band -- a cron job, a scheduled workflow, an operator command -- the
same way `loomwork.archive` is run out of band; it is not part of the engine's hot path.

Pass `dry_run=True` to see what would be deleted (the returned report still counts it)
without deleting anything, and `prefix=...` to restrict a scan to blobs written under a
particular `OffloadCodec.key_prefix`, if a blob store is shared with unrelated data.

**Archives.** History archives (`loomwork archive`, see
[archiving](operations/archiving.md)) keep payloads exactly as stored, so an archived run
refers to its offloaded blobs by key. `collect_garbage` only sees runs still in the
store: once runs are archived and pruned, their blobs look unreferenced. If archives must
stay readable, either run offloading with a `key_prefix` and exclude archived prefixes
from collection, or keep blobs at least as long as the archives by giving
`collect_garbage` a `grace_period` no shorter than your archive retention.

## See also

- `docs/payload-encryption.md` -- the codec chain, `EncryptionCodec`'s threat model, and
  how `OffloadCodec` composes with it.
- `internals.md`, "Payloads and codecs" -- where the codec chain runs relative to the
  engine and the store.
