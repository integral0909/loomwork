"""Encryption key management.

A :class:`KeyRing` answers two questions for :class:`~loomwork.codec.encryption.
EncryptionCodec`: which key to encrypt new payloads with (the *current* key), and how to
find a key that a previously-encrypted payload names (by id). Keeping old keys around
after :data:`current_key_id` moves on is what makes key **rotation** safe: payloads
written under a retired key keep decoding, new payloads pick up the new one.

Key material never appears in a ``repr`` or in an exception message; only key ids do.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import stat
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from loomwork.codec.errors import UnknownKeyError

KEY_BYTES = 32  # AES-256
ENV_KEY_PREFIX = "LOOMWORK_KEY_"
ENV_CURRENT_KEY = "LOOMWORK_CURRENT_KEY"


class InsecureKeyFilePermissionsWarning(UserWarning):
    """A :class:`FileKeyRing` file is readable or writable by group or other."""


@dataclass(frozen=True, slots=True, init=False)
class Key:
    """A single symmetric key.

    Attributes:
        id: opaque identifier stored alongside ciphertext so it can be looked up again
            at decode time. Never secret.
        material: raw key bytes. Must be exactly :data:`KEY_BYTES` long (AES-256).
    """

    id: str
    material: bytes

    def __init__(self, id: str, material: bytes) -> None:
        if len(material) != KEY_BYTES:
            raise ValueError(f"key {id!r} must be {KEY_BYTES} bytes, got {len(material)}")
        object.__setattr__(self, "id", id)
        object.__setattr__(self, "material", material)

    def __repr__(self) -> str:
        return f"Key(id={self.id!r}, material=<redacted>)"


@runtime_checkable
class KeyRing(Protocol):
    """Resolves the key used to encrypt new payloads and looks keys up by id."""

    @property
    def current_key_id(self) -> str: ...

    def current_key(self) -> Key:
        """The key new payloads are encrypted with."""
        ...

    def get_key(self, key_id: str) -> Key:
        """Look up a key by id, for decoding. Raises :class:`UnknownKeyError`."""
        ...


def _decode_material(key_id: str, material: bytes | str) -> bytes:
    if isinstance(material, bytes):
        return material
    try:
        return base64.b64decode(material, validate=True)
    except binascii.Error as exc:
        raise ValueError(f"key {key_id!r} is not valid base64: {exc}") from exc


@dataclass(frozen=True, slots=True, init=False)
class StaticKeyRing:
    """A fixed, in-code set of keys.

    Suitable for tests and for deployments that already have their own secret
    management wired into application startup. ``keys`` maps key id to either raw bytes
    or a base64-encoded string.
    """

    keys: Mapping[str, Key]
    current_key_id: str

    def __init__(self, keys: Mapping[str, bytes | str], current: str) -> None:
        resolved = {
            key_id: Key(key_id, _decode_material(key_id, material))
            for key_id, material in keys.items()
        }
        if current not in resolved:
            raise ValueError(f"current key id {current!r} is not among the provided keys")
        object.__setattr__(self, "keys", resolved)
        object.__setattr__(self, "current_key_id", current)

    def current_key(self) -> Key:
        return self.keys[self.current_key_id]

    def get_key(self, key_id: str) -> Key:
        try:
            return self.keys[key_id]
        except KeyError:
            raise UnknownKeyError(key_id) from None

    def rotated(self, key_id: str, material: bytes | str) -> StaticKeyRing:
        """Return a new ring with ``key_id`` added and made current.

        The previous current key (and every other key already on the ring) stays
        available, so payloads it already encrypted keep decoding.
        """
        merged: dict[str, bytes | str] = {k: v.material for k, v in self.keys.items()}
        merged[key_id] = material
        return StaticKeyRing(merged, key_id)


@dataclass(frozen=True, slots=True, init=False)
class EnvironmentKeyRing:
    """Reads keys from environment variables.

    Each key is ``LOOMWORK_KEY_<id>=<base64 material>``; ``LOOMWORK_CURRENT_KEY=<id>``
    selects which one new payloads are encrypted with. This is the usual choice for
    containerised deployments where secrets are injected as environment variables.
    """

    _inner: StaticKeyRing

    def __init__(self, *, env: Mapping[str, str] | None = None) -> None:
        source = env if env is not None else os.environ
        keys = {
            name[len(ENV_KEY_PREFIX) :]: value
            for name, value in source.items()
            if name.startswith(ENV_KEY_PREFIX) and len(name) > len(ENV_KEY_PREFIX)
        }
        if not keys:
            raise ValueError(
                f"no environment variables starting with {ENV_KEY_PREFIX!r} were found"
            )
        current = source.get(ENV_CURRENT_KEY)
        if not current:
            raise ValueError(f"{ENV_CURRENT_KEY} is not set")
        object.__setattr__(self, "_inner", StaticKeyRing(keys, current))

    @property
    def current_key_id(self) -> str:
        return self._inner.current_key_id

    def current_key(self) -> Key:
        return self._inner.current_key()

    def get_key(self, key_id: str) -> Key:
        return self._inner.get_key(key_id)


@dataclass(frozen=True, slots=True, init=False)
class FileKeyRing:
    """Reads keys from a JSON file.

    The file holds ``{"current": "<id>", "keys": {"<id>": "<base64 material>", ...}}``.
    Because the file contains key material, its permissions are checked on load: a mode
    that grants group or other read/write access triggers
    :class:`InsecureKeyFilePermissionsWarning` (the ring still loads -- operators may be
    inspecting a copy -- but production deployments should treat the warning as a bug).
    """

    path: Path
    _inner: StaticKeyRing

    def __init__(self, path: str | Path) -> None:
        file_path = Path(path)
        _warn_if_insecure(file_path)
        try:
            raw = json.loads(file_path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise ValueError(f"cannot read key file {file_path}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"key file {file_path} is not valid JSON: {exc}") from exc
        try:
            keys = raw["keys"]
            current = raw["current"]
        except (KeyError, TypeError) as exc:
            raise ValueError(
                f"key file {file_path} must be an object with 'keys' and 'current'"
            ) from exc
        object.__setattr__(self, "path", file_path)
        object.__setattr__(self, "_inner", StaticKeyRing(keys, current))

    @property
    def current_key_id(self) -> str:
        return self._inner.current_key_id

    def current_key(self) -> Key:
        return self._inner.current_key()

    def get_key(self, key_id: str) -> Key:
        return self._inner.get_key(key_id)


def _warn_if_insecure(path: Path) -> None:
    try:
        mode = path.stat().st_mode
    except OSError:
        return  # Missing/unreadable file is reported by the JSON load that follows.
    loose = mode & (stat.S_IRWXG | stat.S_IRWXO)
    if loose:
        warnings.warn(
            f"key file {path} has permissions {oct(mode & 0o777)}, which grants group "
            "or other access; restrict it to the owner only (chmod 600)",
            InsecureKeyFilePermissionsWarning,
            stacklevel=3,
        )
