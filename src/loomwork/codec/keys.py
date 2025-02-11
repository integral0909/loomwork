"""Operator helpers for generating encryption keys.

Run as a script to print a new key for use with :class:`~loomwork.codec.keyring.
EnvironmentKeyRing` or :class:`~loomwork.codec.keyring.FileKeyRing`::

    python -m loomwork.codec.keys generate
"""

from __future__ import annotations

import base64
import secrets
import sys
from collections.abc import Sequence

from loomwork.codec.keyring import KEY_BYTES


def generate_key() -> str:
    """Return a new random, base64-encoded AES-256 key."""
    return base64.b64encode(secrets.token_bytes(KEY_BYTES)).decode("ascii")


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args != ["generate"]:
        print("usage: python -m loomwork.codec.keys generate", file=sys.stderr)  # noqa: T201
        return 2
    print(generate_key())  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
