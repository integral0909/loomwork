"""``loomwork token ...``: manage the server's token file.

The token file holds salted hashes only. A token's plaintext is printed once, when it is
created, and cannot be recovered afterwards: lose it and create a new one.
"""

from __future__ import annotations

import argparse
import contextlib
import os
from pathlib import Path
from typing import Any

from loomwork.cli.errors import ExitCode, UsageError
from loomwork.cli.output import Console
from loomwork.server.auth import Role, TokenFile, generate_token


def _load(path: Path, *, create: bool = False) -> TokenFile:
    if not path.exists():
        if create:
            return TokenFile()
        raise UsageError(f"token file not found: {path}")
    try:
        return TokenFile.load(path)
    except (ValueError, KeyError) as exc:
        raise UsageError(f"{path} is not a valid token file: {exc}") from None


def _save(tokens: TokenFile, path: Path) -> None:
    """Write atomically and readable by the owner only: the hashes are not secrets in
    themselves, but there is no reason for anyone else to read them."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    tokens.save(temporary)
    with contextlib.suppress(OSError):
        os.chmod(temporary, 0o600)
    os.replace(temporary, path)


async def create(args: argparse.Namespace, console: Console) -> int:
    path = Path(args.file)
    tokens = _load(path, create=True)
    if args.id in tokens.tokens and not args.replace:
        raise UsageError(f"token {args.id!r} exists; pass --replace to rotate it")
    try:
        plaintext, record = generate_token(
            args.id, args.role, args.namespace or ["default"], description=args.description
        )
    except ValueError as exc:
        raise UsageError(str(exc)) from None
    tokens.tokens[record.id] = record
    _save(tokens, path)
    if args.output:
        output = Path(args.output)
        # Created owner-only from the start, so the secret is never briefly readable.
        descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(plaintext + "\n")
        console.note(f"token {record.id} written to {output}")
    elif console.json_output:
        console.json({"token": plaintext, "record": record.to_dict()})
    else:
        console.print(plaintext)
        console.note(f"token {record.id} saved to {path}; the value above is not shown again")
    return ExitCode.OK


async def list_tokens(args: argparse.Namespace, console: Console) -> int:
    tokens = _load(Path(args.file))
    records = sorted(tokens.tokens.values(), key=lambda r: r.id)
    console.table(
        ["id", "roles", "namespaces", "description"],
        [
            [r.id, ",".join(role.value for role in r.roles), ",".join(r.namespaces), r.description]
            for r in records
        ],
        # Never print the hashes, even in JSON: listing is for people, not for copying.
        records=[
            {
                "id": r.id,
                "roles": [role.value for role in r.roles],
                "namespaces": list(r.namespaces),
                "description": r.description,
            }
            for r in records
        ],
        empty="no tokens",
    )
    return ExitCode.OK


async def revoke(args: argparse.Namespace, console: Console) -> int:
    path = Path(args.file)
    tokens = _load(path)
    missing = [token_id for token_id in args.ids if token_id not in tokens.tokens]
    if missing:
        raise UsageError(f"no such token: {', '.join(missing)}")
    for token_id in args.ids:
        del tokens.tokens[token_id]
    _save(tokens, path)
    console.note(f"revoked {', '.join(args.ids)}; running servers apply this within a second")
    return ExitCode.OK


async def verify(args: argparse.Namespace, console: Console) -> int:
    """Check a token against the file without starting a server."""
    tokens = _load(Path(args.file))
    text = console.read_stdin() if args.token == "-" else args.token  # noqa: S105
    if not text:
        raise UsageError("no token given")
    from loomwork.server.auth import InvalidTokenError

    try:
        record = tokens.authenticate(text.strip())
    except InvalidTokenError as exc:
        console.error(str(exc))
        return ExitCode.FAILURE
    if console.json_output:
        console.json({"id": record.id, "roles": [r.value for r in record.roles]})
    else:
        console.print(f"valid: {record.id} ({', '.join(r.value for r in record.roles)})")
    return ExitCode.OK


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser("token", help="manage server bearer tokens")
    commands = parser.add_subparsers(dest="token_command", metavar="COMMAND", required=True)

    def file_argument(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--file",
            "-f",
            default=os.environ.get("LOOMWORK_TOKENS_FILE", "tokens.json"),
            help="token file (default: $LOOMWORK_TOKENS_FILE or ./tokens.json)",
        )

    p = commands.add_parser("create", help="create a token and print it once")
    p.add_argument("id", help="token id: letters, digits and -")
    p.add_argument(
        "--role",
        action="append",
        required=True,
        choices=[r.value for r in Role],
        help="role to grant (repeatable)",
    )
    p.add_argument(
        "--namespace",
        action="append",
        help="namespace the token may use (repeatable; '*' for all; default: default)",
    )
    p.add_argument("--description", default="")
    p.add_argument("--replace", action="store_true", help="rotate an existing token")
    p.add_argument("--output", "-o", metavar="PATH", help="write the token here instead of stdout")
    file_argument(p)
    p.set_defaults(handler=create)

    p = commands.add_parser("list", aliases=["ls"], help="list tokens (never their secrets)")
    file_argument(p)
    p.set_defaults(handler=list_tokens)

    p = commands.add_parser("revoke", help="delete tokens")
    p.add_argument("ids", nargs="+", metavar="ID")
    file_argument(p)
    p.set_defaults(handler=revoke)

    p = commands.add_parser("verify", help="check a token (read from stdin) against the file")
    p.add_argument(
        "token",
        nargs="?",
        default="-",
        help="the token; default '-' reads it from stdin, which keeps it out of the "
        "process list",
    )
    file_argument(p)
    p.set_defaults(handler=verify)
