"""Where a command finds its store: a database URL or a Loomwork server.

Every value can come from a flag or an environment variable; flags win:

=====================  =========================  =====================================
Flag                   Environment variable       Meaning
=====================  =========================  =====================================
``--store``            ``LOOMWORK_STORE_URL``     ``sqlite:///path``, ``postgresql://``
``--server``           ``LOOMWORK_SERVER_URL``    base URL of a ``loomwork server``
``--token-file``       ``LOOMWORK_TOKEN_FILE``    file holding the bearer token
(none)                 ``LOOMWORK_TOKEN``         the bearer token itself
``--namespace``        ``LOOMWORK_NAMESPACE``     server namespace (default ``default``)
``--attr-type``        ``LOOMWORK_SEARCH_ATTRIBUTES``  search attribute types for queries
=====================  =========================  =====================================

The token deliberately has no flag of its own: command lines end up in shell history and
in the process list, where other users of the machine can read them.
"""

from __future__ import annotations

import argparse
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from loomwork.cli.errors import CliError, ExitCode, UsageError
from loomwork.cli.loader import AppSpec
from loomwork.client.client import Client
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.engine.config import EngineConfig
from loomwork.engine.engine import Engine
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store
from loomwork.store.factory import StoreURLError, redact_url, store_from_url
from loomwork.visibility.schema import AttributeType, SchemaError, SearchAttributes


class StoreUnavailableError(CliError):
    def __init__(self, message: str) -> None:
        super().__init__(message, ExitCode.UNAVAILABLE)


@dataclass(frozen=True, slots=True)
class ConnectionOptions:
    store_url: str | None = None
    server_url: str | None = None
    token: str | None = None
    namespace: str = "default"
    timeout: float = 30.0
    search_attributes: SearchAttributes = field(default_factory=lambda: SearchAttributes({}))

    @property
    def target(self) -> str:
        """A description safe to print: no passwords, no tokens."""
        if self.server_url is not None:
            return f"{self.server_url} (namespace {self.namespace})"
        if self.store_url is not None:
            return redact_url(self.store_url)
        return "nothing"

    @classmethod
    def from_args(
        cls, args: argparse.Namespace, env: Mapping[str, str] | None = None
    ) -> ConnectionOptions:
        environ = os.environ if env is None else env
        store_url = getattr(args, "store", None) or environ.get("LOOMWORK_STORE_URL") or None
        server_url = getattr(args, "server", None) or environ.get("LOOMWORK_SERVER_URL") or None
        if getattr(args, "store", None) and getattr(args, "server", None):
            raise UsageError("give either --store or --server, not both")
        if store_url and server_url:
            # One came from the environment; the explicit flag wins.
            if getattr(args, "store", None):
                server_url = None
            elif getattr(args, "server", None):
                store_url = None
            else:
                raise UsageError(
                    "both LOOMWORK_STORE_URL and LOOMWORK_SERVER_URL are set; unset one "
                    "or pass --store/--server"
                )
        token_file = getattr(args, "token_file", None) or environ.get("LOOMWORK_TOKEN_FILE")
        token = environ.get("LOOMWORK_TOKEN") or None
        if token_file:
            token = _read_token(Path(token_file))
        namespace = (
            getattr(args, "namespace", None) or environ.get("LOOMWORK_NAMESPACE") or "default"
        )
        timeout = float(getattr(args, "timeout", None) or 30.0)
        declarations = [
            *_split_declarations(environ.get("LOOMWORK_SEARCH_ATTRIBUTES", "")),
            *(getattr(args, "attr_type", None) or []),
        ]
        return cls(
            store_url,
            server_url,
            token,
            namespace,
            timeout,
            parse_attribute_types(declarations),
        )


def _split_declarations(text: str) -> list[str]:
    return [part.strip() for part in text.split(",") if part.strip()]


def parse_attribute_types(declarations: list[str]) -> SearchAttributes:
    """``["tier=string", "amount=float"]`` as a :class:`SearchAttributes` schema, so
    visibility queries can reference ``attr.tier`` and ``attr.amount``."""
    types: dict[str, AttributeType] = {}
    valid = ", ".join(t.value for t in AttributeType)
    for declaration in declarations:
        name, sep, type_name = declaration.partition("=")
        if not sep or not name.strip():
            raise UsageError(f"search attribute types look like NAME=TYPE, got {declaration!r}")
        try:
            types[name.strip()] = AttributeType(type_name.strip().lower())
        except ValueError:
            raise UsageError(
                f"unknown search attribute type {type_name!r} for {name!r} (use {valid})"
            ) from None
    try:
        return SearchAttributes.from_mapping(types)
    except SchemaError as exc:
        raise UsageError(str(exc)) from None


def _read_token(path: Path) -> str:
    try:
        token = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        raise UsageError(f"token file not found: {path}") from None
    except OSError as exc:
        raise UsageError(f"cannot read token file {path}: {exc.strerror}") from None
    if not token:
        raise UsageError(f"token file {path} is empty")
    return token


def add_connection_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("connection")
    group.add_argument("--store", metavar="URL", help="store URL (default: $LOOMWORK_STORE_URL)")
    group.add_argument(
        "--server", metavar="URL", help="Loomwork server URL (default: $LOOMWORK_SERVER_URL)"
    )
    group.add_argument(
        "--token-file",
        metavar="PATH",
        help="file holding the server bearer token (default: $LOOMWORK_TOKEN_FILE, "
        "or the token itself in $LOOMWORK_TOKEN)",
    )
    group.add_argument(
        "--namespace", metavar="NAME", help="server namespace (default: $LOOMWORK_NAMESPACE)"
    )
    group.add_argument(
        "--attr-type",
        action="append",
        metavar="NAME=TYPE",
        help="declare a search attribute's type for queries, e.g. tier=string (repeatable; "
        "also $LOOMWORK_SEARCH_ATTRIBUTES as a comma-separated list)",
    )
    group.add_argument(
        "--timeout",
        type=float,
        metavar="SECONDS",
        help="server request timeout (default: 30)",
    )


@dataclass(slots=True)
class Session:
    """An open store with an engine and client on top."""

    store: Store
    engine: Engine
    client: Client
    converter: DataConverter
    options: ConnectionOptions


async def open_store(options: ConnectionOptions) -> Store:
    if options.server_url is not None:
        from loomwork.remote.connect import connect

        try:
            return await connect(
                options.server_url,
                options.token,
                options.namespace,
                timeout=options.timeout,
            )
        except OSError as exc:
            raise StoreUnavailableError(
                f"cannot reach server {options.server_url}: {exc}"
            ) from None
    if options.store_url is None:
        raise UsageError(
            "no store configured: pass --store URL or --server URL, or set "
            "LOOMWORK_STORE_URL or LOOMWORK_SERVER_URL"
        )
    try:
        store = store_from_url(options.store_url)
    except StoreURLError as exc:
        raise UsageError(f"invalid store URL: {exc}") from None
    except ImportError as exc:
        raise UsageError(
            f"{exc}; install the matching extra (loomwork[sqlite] or [postgres])"
        ) from None
    try:
        await store.open()
    except OSError as exc:
        raise StoreUnavailableError(f"cannot open store {options.target}: {exc}") from None
    return store


@asynccontextmanager
async def session(options: ConnectionOptions, app: AppSpec | None = None) -> AsyncIterator[Session]:
    """Open the configured store for the duration of one command."""
    store = await open_store(options)
    try:
        config = EngineConfig(search_attributes=options.search_attributes)
        engine = Engine(store, clock=getattr(store, "clock", None), config=config)
        converter = app.converter if app is not None else default_converter
        registry: Registry | None = app.registry if app is not None else None
        client = Client(engine, converter, registry=registry)
        yield Session(store, engine, client, converter, options)
    finally:
        await store.close()
