"""Payload envelope, value conversion and the payload codec chain.

Every user value that crosses the engine boundary (workflow arguments, step results,
failure details) is converted to a :class:`Payload` by a :class:`DataConverter`, then
optionally run through a :class:`PayloadCodec` -- compression, encryption, size
validation, or a :class:`CodecChain` of several -- before it reaches the store. See
``docs/payload-encryption.md`` for the encryption threat model and setup.

:class:`EncryptionCodec` needs the optional ``crypto`` dependency group
(``pip install loomwork[crypto]``); :class:`OffloadCodec` needs ``loomwork.blobs`` (see
``docs/large-payloads.md``). Importing this package requires neither -- both names are
resolved lazily on first use -- so encryption and large-payload offloading stay opt-in
for deployments that don't need them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loomwork.codec.base import CodecChain, PayloadCodec
from loomwork.codec.compression import CompressionCodec
from loomwork.codec.converter import DataConverter, PayloadConverter, default_converter
from loomwork.codec.errors import CodecError, IntegrityError, PayloadTooLargeError, UnknownKeyError
from loomwork.codec.keyring import (
    EnvironmentKeyRing,
    FileKeyRing,
    InsecureKeyFilePermissionsWarning,
    Key,
    KeyRing,
    StaticKeyRing,
)
from loomwork.codec.limits import PayloadSizeLimitCodec
from loomwork.codec.payload import Payload

if TYPE_CHECKING:
    from loomwork.codec.encryption import EncryptionCodec
    from loomwork.codec.offload import OffloadCodec

__all__ = [
    "CodecChain",
    "CodecError",
    "CompressionCodec",
    "DataConverter",
    "EncryptionCodec",
    "EnvironmentKeyRing",
    "FileKeyRing",
    "InsecureKeyFilePermissionsWarning",
    "IntegrityError",
    "Key",
    "KeyRing",
    "OffloadCodec",
    "Payload",
    "PayloadCodec",
    "PayloadConverter",
    "PayloadSizeLimitCodec",
    "PayloadTooLargeError",
    "StaticKeyRing",
    "UnknownKeyError",
    "default_converter",
]


def __getattr__(name: str) -> Any:
    # EncryptionCodec pulls in the optional `cryptography` package. Importing it lazily,
    # only when someone actually asks for it, keeps `import loomwork` working without
    # the `crypto` extra installed.
    if name == "EncryptionCodec":
        from loomwork.codec.encryption import EncryptionCodec

        return EncryptionCodec
    # OffloadCodec pulls in loomwork.blobs (and, transitively, loomwork.store for its
    # garbage collector). Resolving it lazily keeps that out of `import loomwork`'s
    # eager import graph, the same reasoning as EncryptionCodec above.
    if name == "OffloadCodec":
        from loomwork.codec.offload import OffloadCodec

        return OffloadCodec
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
