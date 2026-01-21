# Media processing

Large payload offloading ("claim check", `docs/large-payloads.md`): steps whose inputs
and outputs are megabytes of binary data, compressed, encrypted, and moved out of the
workflow's history into a blob store, leaving a small reference behind.

## Scenario

`render_job` (in `workflows.py`) renders an image, then a report that references it:

1. `render_image` returns a ~2.3 MB `bytes` value (a stand-in for a rendered image).
2. `render_report` takes that image *as an argument* -- not just its length -- and
   returns a ~1.2 MB report `bytes` value referencing it. A large value therefore flows
   both out of one step and into the next, and both the argument and the result go
   through the codec chain independently.
3. The workflow's own result (`MediaJobResult`) carries only sizes, never the bytes
   themselves -- the *run's* result stays small even though its steps' data does not.

`main.py` builds the codec chain `docs/large-payloads.md` recommends:

```python
CodecChain([
    CompressionCodec(),
    EncryptionCodec(key_ring),
    OffloadCodec(blob_store, threshold_bytes=128 * 1024),
    PayloadSizeLimitCodec(max_bytes=4096),
])
```

with a `FileSystemBlobStore` in a temporary directory and an `EnvironmentKeyRing` built
from an explicit environment-style mapping (`LOOMWORK_KEY_<id>`, `LOOMWORK_CURRENT_KEY`
-- the same shape a containerized deployment injects as real environment variables; see
`docs/payload-encryption.md`) rather than the real process environment, so the demo
never depends on, or pollutes, `os.environ`. It then:

1. Runs the job and prints what actually reached the store's history: every large
   payload shows up as a reference (`0` bytes of data, plus the blob key) rather than
   the megabytes it stands for; small payloads (like the job id) are kept inline, since
   `OffloadCodec` costs nothing below its threshold.
2. Reads the blob directory directly off disk and confirms the recognizable plaintext
   marker every generated image and report starts with **never** appears in any blob
   file -- only ciphertext does, since `OffloadCodec` runs after compression and
   encryption in the chain.
3. Prunes the (now closed) run with `Store.delete_closed_before` -- the same operation
   `loomwork store prune` performs -- and runs `loomwork.blobs.collect_garbage`
   afterwards, which deletes every blob the pruned run left orphaned.

## Loomwork features used

- `OffloadCodec` (`loomwork.codec.offload`) composed after `CompressionCodec` and
  `EncryptionCodec` in a `CodecChain`, with `PayloadSizeLimitCodec` last.
- `FileSystemBlobStore` and `loomwork.blobs.collect_garbage`.
- `EnvironmentKeyRing` (`loomwork.codec`) for a key ring shaped like a real deployment's
  environment variables.
- `Store.delete_closed_before` (pruning) feeding directly into garbage collection --
  the same "archive/prune, then collect" sequencing `docs/large-payloads.md`'s garbage
  collection section describes.

## Running it

```console
uv run python -m examples.media_processing.main
```

Uses a real `SQLiteStore` in a temporary directory (durability and encryption are the
point) and the real clock -- nothing here waits on durable time, so it finishes in a
fraction of a second despite the megabytes of data involved.

## Tests

`tests/examples/test_media_processing.py` checks:

- every offloaded payload recorded in history is a small reference
  (`loomwork.codec.offload.offload_key` resolves it), never the multi-megabyte value,
  for both a step's result and an argument passed to the next step;
- the blob directory holds ciphertext only -- the plaintext marker embedded in every
  generated image and report never appears in any blob file on disk;
- pruning a closed run and then running `collect_garbage` removes every blob it left
  orphaned.
