# Payload encryption

Every user value that crosses the engine boundary -- workflow arguments, step arguments
and results, and failure `details` -- is converted to a `Payload` (opaque bytes plus
string metadata) before the engine or store ever sees it. The `loomwork.codec` package
lets you run those payloads through a chain of codecs -- compression, encryption, size
validation -- so that what actually reaches the store is exactly what the codec chain
produces. This document covers the encryption codec: the threat model it addresses,
how to set it up, how to rotate keys, and -- just as important -- what it does *not*
protect. See `docs/large-payloads.md` for `OffloadCodec`, which moves large payloads to a
blob store and composes with encryption so the blob store only ever holds ciphertext.

## Threat model

**In scope.** With `EncryptionCodec` enabled, workflow arguments, step arguments and
results, and failure `details` are encrypted (AES-256-GCM) before they leave the process
that produced them and are decrypted only by a process holding the matching key. This
means:

- Anyone with read access to the store (a database administrator, a leaked backup, a
  misconfigured replica) sees ciphertext, not your data.
- Anyone who can read the wire between a worker and a server-mode API sees ciphertext
  (assuming TLS is also in place for the wire itself -- payload encryption is a second,
  independent layer, not a substitute for transport security).
- A worker process that does not have the relevant key cannot execute workflow or step
  code that needs the plaintext, even if it can otherwise reach the store or API.

**Out of scope / trusted.** A worker process that *does* have the key is trusted with
the plaintext, by design -- that is what lets it run your workflow and step code.
Loomwork does not protect payload contents from:

- The workflow or step code itself (see `SECURITY.md`: workflow code is trusted code,
  not sandboxed).
- Anyone with access to a worker process's memory, environment variables, or the key
  material itself (a `FileKeyRing` file, the process environment for
  `EnvironmentKeyRing`, or wherever a `StaticKeyRing`'s keys come from).
- Anyone who compromises the key management system a `KeyRing` implementation is backed
  by.

## What stays in the clear

Turning on encryption encrypts **payload bytes**. It does not encrypt everything
Loomwork records. In particular, the following are always readable by anyone with store
access, encryption or not:

- **Workflow and step names**, workflow and run ids, step sequence numbers, and
  timestamps. These are structural metadata the engine and store need to operate on
  directly; they are never routed through the payload codec chain.
- **Failure `message` and `stack_trace`**. `loomwork.failure.FailureInfo` stores these as
  plain strings in the history, not as payloads (only `details`, the JSON-compatible
  extra data a raiser attaches, goes through the converter and codec chain -- see
  `tests/codec/test_storage_roundtrip.py::TestFailureDetailsThroughTheCodecChain` for
  what that looks like in practice). **Do not put secrets in exception messages.** If a
  failure needs to carry sensitive context, attach it via `details=...` (which is
  encrypted along with everything else) rather than interpolating it into the message
  string or letting it appear in a traceback.
- **Retry and timeout policy values**, queue names, and other options: these describe
  *how* work runs, not the data it operates on.
- **Metadata keys the codec chain itself adds** (encoding markers, key ids, nonces,
  compression algorithm names). None of these carry secret material, but they are
  visible in the clear next to the ciphertext, by design -- see "Associated data" below.

If a value must never be readable by store administrators, put it in an argument,
result, or failure `details` value and make sure `EncryptionCodec` is in the codec
chain; do not put it in a workflow id, a step name, or an exception message.

## Setup

### 1. Install the optional dependency

`EncryptionCodec` needs the `cryptography` package. Install it via the `crypto` extra
(see the note at the end of this document about `pyproject.toml`):

```bash
pip install "loomwork[crypto]"
```

Importing `loomwork` or `loomwork.codec` never requires `cryptography` to be installed;
it is only imported when `EncryptionCodec` is actually constructed, so deployments that
don't need encryption are unaffected.

### 2. Generate a key

```bash
python -m loomwork.codec.keys generate
```

This prints a random, base64-encoded 256-bit key. Treat it like any other secret:
generate it out of band, store it in your secret manager, and never commit it.

### 3. Choose a key ring

A `KeyRing` tells `EncryptionCodec` which key to encrypt new payloads with (the
*current* key) and how to look up a key by id when decrypting. Pick the implementation
that matches how your deployment already manages secrets:

- **`StaticKeyRing`** -- keys given directly in code. Mainly for tests, or for
  deployments that already pull secrets into the process by some other means before
  constructing the ring.

  ```python
  from loomwork.codec import StaticKeyRing

  keyring = StaticKeyRing({"2024-01": "base64-key-material..."}, current="2024-01")
  ```

- **`EnvironmentKeyRing`** -- reads `LOOMWORK_KEY_<id>=<base64 material>` environment
  variables plus `LOOMWORK_CURRENT_KEY=<id>`. The usual choice for containerised
  deployments where secrets are injected as environment variables.

  ```bash
  export LOOMWORK_KEY_2024_01="$(python -m loomwork.codec.keys generate)"
  export LOOMWORK_CURRENT_KEY=2024_01
  ```

  ```python
  from loomwork.codec import EnvironmentKeyRing

  keyring = EnvironmentKeyRing()
  ```

- **`FileKeyRing`** -- reads a JSON file: `{"current": "<id>", "keys": {"<id>":
  "<base64 material>"}}`. On load, the file's permissions are checked; a mode that
  grants group or other read/write triggers `InsecureKeyFilePermissionsWarning` (the
  ring still loads, but treat the warning as a bug to fix -- `chmod 600` the file).

  ```python
  from loomwork.codec import FileKeyRing

  keyring = FileKeyRing("/etc/loomwork/keys.json")
  ```

Every key id given to a `KeyRing` implementation must decode to exactly 32 bytes (AES-256).

### 4. Build the codec chain and attach it to a `DataConverter`

Codecs run in the order given on encode, and in reverse order on decode. A typical
chain compresses first (so encryption operates on the smaller ciphertext-adjacent input
and doesn't waste effort trying to compress already-random-looking ciphertext), encrypts
second, and enforces a size limit last, since the limit should apply to what actually
gets written to the store:

```python
from loomwork.codec import (
    CodecChain,
    CompressionCodec,
    DataConverter,
    EncryptionCodec,
    PayloadSizeLimitCodec,
)

chain = CodecChain([
    CompressionCodec(),
    EncryptionCodec(keyring),
    PayloadSizeLimitCodec(max_bytes=2 * 1024 * 1024),
])
converter = DataConverter(codec=chain)
```

`DataConverter`'s existing synchronous methods (`encode`, `decode`, `encode_args`,
`decode_args`, `decode_result`) are unaffected by attaching a codec -- they only ever do
value <-> bytes conversion, which is what makes them safe to call from workflow
replay, where no I/O is allowed. The codec chain runs through the new async methods
(`encode_async`, `decode_async`, `encode_args_async`, `decode_args_async`,
`decode_result_async`), since a codec may need to call out to a key management service.
Wiring the engine's argument/result path to the async methods is a separate piece of
work from this document; this converter is where that wiring will plug in.

### Enabling encryption on an existing deployment

You do not need a migration. Every codec marks the payloads it produces with a
distinctive encoding in the payload's metadata; decoding a payload that does not carry a
codec's marker returns it unchanged. This means turning on `EncryptionCodec` (or adding
any codec) does not break replay of history recorded before the change: old, unencrypted
payloads decode exactly as before, and only new payloads get encrypted. See
`tests/codec/test_storage_roundtrip.py::test_history_recorded_without_a_codec_still_replays_after_one_is_added`.

## Key rotation

Rotation means moving to a new *current* key for new payloads while keeping old keys
around so payloads already encrypted with them keep decoding. Every `KeyRing`
implementation supports this by design: `current_key()` is used for encoding,
`get_key(key_id)` is used for decoding and accepts any key the ring still knows about.

1. **Generate a new key**: `python -m loomwork.codec.keys generate`.
2. **Add it to the ring, keeping the old key**:
   - `StaticKeyRing`: call `.rotated(new_id, new_material)`, which returns a new ring
     with the new key made current and every previously known key still resolvable.
   - `EnvironmentKeyRing`: add a new `LOOMWORK_KEY_<new-id>` variable and repoint
     `LOOMWORK_CURRENT_KEY` to it. Keep the old `LOOMWORK_KEY_<old-id>` variable set.
   - `FileKeyRing`: add an entry to the file's `"keys"` object and change `"current"`.
     Keep the old entry.
3. **Roll out the updated ring to every worker** before removing anything. Workers that
   still have only the old ring can keep decoding new payloads only once they pick up
   the new key; workers with the updated ring can decode both old and new payloads
   immediately.
4. **Retire the old key** only once you are confident nothing still needs to decode
   payloads written under it (for an event-sourced system, that generally means every
   workflow run whose history contains payloads encrypted with the old key has
   completed, or you are prepared to lose the ability to replay them). Retiring a key
   means removing it from the ring; until then, leaving it in place costs nothing and
   is the safer default.

There is no server-side "re-encrypt everything" step: rotation is entirely a property of
which keys a `KeyRing` knows about at any given moment, so it works incrementally, in
place, on a live deployment.

## Associated data and tamper detection

`EncryptionCodec` authenticates more than the ciphertext: the metadata that will
accompany it (the codec's own key id, nonce and prior-encoding bookkeeping, plus
whatever else was already on the payload) is passed to AES-GCM as associated data. This
means that after encryption, changing *either* the ciphertext *or* any of that metadata
-- including metadata a different part of the system might later attach -- is detected
on decode and raises `loomwork.codec.errors.IntegrityError`, distinct from
`UnknownKeyError` (a key id that is not present in the ring, a configuration problem,
not evidence of tampering).

## Downgrade protection (`require_encryption`)

By default, `EncryptionCodec.decode` passes through payloads it did not encrypt. That is
what lets you switch encryption on for an existing deployment: runs recorded before the
switch still replay.

The flip side is that someone with **write** access to the store could replace a
ciphertext payload with a plaintext payload of their choosing, and it would be accepted.
Once every run that predates encryption has closed (or been retired with
`Store.delete_closed_before`), enable strict mode:

```python
codec = EncryptionCodec(keyring, require_encryption=True)
```

With strict mode on, decoding any payload that is not AES-GCM encrypted raises
`IntegrityError`, exactly like a tampered ciphertext.

## Installation

Encryption needs the optional `crypto` extra:

```bash
pip install "loomwork[crypto]"
```

`import loomwork` works without it; only accessing `loomwork.codec.EncryptionCodec`
requires the `cryptography` package.
