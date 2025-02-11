"""Errors raised by the payload codec chain.

These sit alongside :class:`loomwork.errors.PayloadError` (which covers value <-> bytes
conversion) but describe failures in the layer that runs *after* conversion and *before*
storage: compression, encryption and size validation.
"""

from __future__ import annotations

from loomwork.errors import LoomworkError


class CodecError(LoomworkError):
    """Base class for payload codec failures."""


class UnknownKeyError(CodecError):
    """A payload referenced a key id that its :class:`~loomwork.codec.keyring.KeyRing`
    does not know about.

    This happens when a payload was encrypted with a key that has since been removed
    from the ring, or when a worker's key configuration is incomplete. It is always a
    configuration problem, never evidence of tampering.
    """

    def __init__(self, key_id: str) -> None:
        super().__init__(f"unknown key id {key_id!r}")
        self.key_id = key_id


class IntegrityError(CodecError):
    """A payload failed an authenticity check.

    Raised when authenticated decryption fails: the ciphertext, nonce or metadata bound
    to it (associated data) was altered after encryption, or the wrong key was used.
    Unlike :class:`UnknownKeyError`, this indicates the bytes cannot be trusted.
    """


class PayloadTooLargeError(CodecError):
    """An encoded payload exceeds the configured size limit."""

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"payload of {size} bytes exceeds the {limit} byte limit")
        self.size = size
        self.limit = limit
