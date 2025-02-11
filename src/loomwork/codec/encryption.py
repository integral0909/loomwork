"""AES-256-GCM payload encryption.

Requires the optional ``crypto`` dependency group (``cryptography>=43,<44``; see
``pyproject.toml``). See ``docs/payload-encryption.md`` for the threat model, setup and
key rotation procedure.
"""

from __future__ import annotations

import base64
import os
from collections.abc import Sequence
from dataclasses import dataclass

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from loomwork.codec.base import produced_by, unwrap
from loomwork.codec.errors import IntegrityError
from loomwork.codec.keyring import KeyRing
from loomwork.codec.payload import ENCODING_KEY, Payload

ENCODING_ENCRYPTED = "binary/encrypted"

# AES-GCM is designed around a 96-bit nonce; using any other size forces GHASH to derive
# one, which loses some of the scheme's guarantees. A new nonce is drawn for every
# payload, never reused under the same key.
NONCE_BYTES = 12

_INNER_ENCODING_KEY = "codec.encryption.inner-encoding"
_KEY_ID_KEY = "codec.encryption.key-id"
_NONCE_KEY = "codec.encryption.nonce"


@dataclass(slots=True)
class EncryptionCodec:
    """Encrypts payload bytes with AES-256-GCM.

    The metadata that will accompany the ciphertext (key id, nonce, prior encoding, and
    anything already on the payload) is authenticated as AEAD associated data, so
    tampering with metadata after encryption is caught on decode exactly like tampering
    with the ciphertext -- both raise :class:`~loomwork.codec.errors.IntegrityError`.

    Key lookup goes through a :class:`~loomwork.codec.keyring.KeyRing`: encoding always
    uses the ring's current key; decoding looks up whatever key id the payload names,
    which may be a key that has since been rotated out. This is what makes rotation
    safe -- old history keeps decoding under retired keys while new payloads move to the
    new one.
    """

    keyring: KeyRing
    #: Reject payloads that are not encrypted instead of passing them through. Enable it
    #: once no unencrypted history remains: otherwise someone with write access to the
    #: store could replace a ciphertext with a plaintext payload of their choosing.
    require_encryption: bool = False

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [self._encrypt(p) for p in payloads]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [self._decrypt(p) for p in payloads]

    def _encrypt(self, payload: Payload) -> Payload:
        key = self.keyring.current_key()
        nonce = os.urandom(NONCE_BYTES)
        metadata = {
            **payload.metadata,
            _KEY_ID_KEY: key.id,
            _NONCE_KEY: _b64(nonce),
            _INNER_ENCODING_KEY: payload.encoding,
            ENCODING_KEY: ENCODING_ENCRYPTED,
        }
        ciphertext = AESGCM(key.material).encrypt(nonce, payload.data, _associated_data(metadata))
        return Payload(ciphertext, metadata)

    def _decrypt(self, payload: Payload) -> Payload:
        if not produced_by(payload, ENCODING_ENCRYPTED):
            if self.require_encryption:
                raise IntegrityError(
                    f"refusing unencrypted payload (encoding {payload.encoding!r}) because "
                    "require_encryption is enabled"
                )
            return payload
        try:
            key_id = payload.metadata[_KEY_ID_KEY]
            nonce = base64.b64decode(payload.metadata[_NONCE_KEY])
        except KeyError as exc:
            raise IntegrityError(f"encrypted payload is missing {exc} metadata") from None
        key = self.keyring.get_key(key_id)
        try:
            plaintext = AESGCM(key.material).decrypt(
                nonce, payload.data, _associated_data(payload.metadata)
            )
        except InvalidTag as exc:
            raise IntegrityError(
                "payload failed authentication: the ciphertext or its metadata was "
                "modified after encryption, or the wrong key was used"
            ) from exc
        return unwrap(
            payload,
            plaintext,
            inner_encoding_key=_INNER_ENCODING_KEY,
            drop_keys=(_KEY_ID_KEY, _NONCE_KEY),
        )


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _associated_data(metadata: dict[str, str]) -> bytes:
    """A canonical, collision-resistant byte encoding of ``metadata``.

    Length-prefixing each key and value (rather than joining with a separator such as
    ``=`` or ``\\n``) means two different metadata dicts can never serialize to the same
    bytes, so an attacker cannot shuffle characters between fields to forge a payload
    that authenticates under different metadata.
    """
    parts: list[bytes] = []
    for key, value in sorted(metadata.items()):
        key_bytes, value_bytes = key.encode("utf-8"), value.encode("utf-8")
        parts.append(len(key_bytes).to_bytes(4, "big"))
        parts.append(key_bytes)
        parts.append(len(value_bytes).to_bytes(4, "big"))
        parts.append(value_bytes)
    return b"".join(parts)
