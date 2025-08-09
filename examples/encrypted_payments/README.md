# Encrypted payments

Payload encryption end to end -- arguments, a child workflow's own arguments, a signal
and the final result -- plus key rotation with no downtime and no migration step.

## Scenario

`process_payment` (in `workflows.py`) takes a card number as an argument, passes it to
a step, to a child workflow, and receives it again through a `Channel` signal. None of
that is special-cased: every one of those values is converted to a `Payload` by the
`DataConverter` and, in this example, run through a codec chain of compression then
encryption before it ever reaches the store.

`main.py` builds that chain the way `docs/payload-encryption.md` recommends:

```python
DataConverter(codec=CodecChain([CompressionCodec(threshold=64), EncryptionCodec(ring)]))
```

with a `StaticKeyRing` holding a key generated at runtime with
`loomwork.codec.keys.generate_key()` (never committed, never logged). It then:

1. Processes a payment under key `2024-q1` and shows that the raw `SQLiteStore` file
   contains no trace of the card number -- only an encryption marker.
2. **Rotates** to a new key, `2024-q2`, with `ring.rotated(...)`, which keeps `2024-q1`
   resolvable. A second payment is encrypted under the new key; the first payment's
   history, still encrypted under the old one, keeps decoding without any migration --
   rotation is a property of what the `KeyRing` knows right now, not something applied
   to already-written data.
3. **Retires** `2024-q1` (builds a ring that no longer has it) and shows that the first
   payment's result can no longer be decoded -- `UnknownKeyError` -- which is exactly
   why a real rotation keeps old keys around until every run that used them has closed.

## Loomwork features used

- `CodecChain`, `CompressionCodec` and `EncryptionCodec` (`loomwork.codec`) attached to
  a `DataConverter` passed to `Loomwork`.
- `StaticKeyRing` and `.rotated()` for key rotation; `loomwork.codec.keys.generate_key()`
  for generating key material without ever hard-coding it.
- Encryption applying uniformly to workflow arguments, a child workflow's arguments, a
  channel signal and the workflow result.

## Running it

```console
uv run python -m examples.encrypted_payments.main
```

This example is specifically about durability and encryption, so it uses a real
`SQLiteStore` in a temporary directory (per the guidance that examples use the
in-memory store except when durability or encryption is the point) rather than
`MemoryStore`. Nothing here waits on time passing, so it runs against the real clock
and finishes in a fraction of a second.

## Tests

`tests/examples/test_encrypted_payments.py` checks:

- a processed payment's result round-trips correctly;
- the raw store file contains no plaintext card number, under either key;
- a rotated ring still decodes history written under the previous key;
- a ring that no longer has the old key raises `UnknownKeyError` when asked to decode
  history written under it.
