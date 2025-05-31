"""Shared fixtures for server tests.

``running_app`` builds a server in-process (no real socket, via ``httpx.ASGITransport``)
and enters its lifespan (opening namespace stores and, optionally, their system workers).
``http_store_over`` layers an :class:`HttpStore` on top, sharing the app's clock so that
tests using a fixed reference time (``tests.helpers.T0``) get it reflected exactly in the
server's own clock -- see the module docstring of
``tests/server/test_http_store_conformance.py`` for why that works.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

from loomwork.clock import Clock, ManualClock
from loomwork.remote.http_store import HttpStore
from loomwork.server.app import create_app
from loomwork.server.auth import TokenFile
from loomwork.server.config import AuthConfig, ServerConfig
from tests.helpers import T0


@asynccontextmanager
async def running_app(
    config: ServerConfig, *, clock: Clock | None = None, run_system_workers: bool = False
):
    app = create_app(config, clock=clock, run_system_workers=run_system_workers)
    async with app.router.lifespan_context(app):
        yield app


@asynccontextmanager
async def http_store_over(
    app, *, token: str | None = None, namespace: str = "default", long_poll: float = 0.0, clock=None
) -> AsyncIterator[HttpStore]:
    transport = httpx.ASGITransport(app=app)
    store = HttpStore(
        "http://test", token, namespace, transport=transport, long_poll=long_poll, clock=clock
    )
    await store.open()
    try:
        yield store
    finally:
        await store.close()


def memory_config(**overrides: object) -> ServerConfig:
    return ServerConfig(
        namespaces={"default": "memory://"},
        auth=AuthConfig(disabled=True),
        **overrides,  # type: ignore[arg-type]
    )


@pytest.fixture
async def store() -> AsyncIterator[HttpStore]:
    """An :class:`HttpStore` over an in-process server backed by :class:`MemoryStore`,
    sharing a frozen :class:`ManualClock` with the server so absolute timestamps a test
    passes round-trip exactly (see ``docs/server.md``, "testing against a fixed clock")."""
    clock = ManualClock(start=T0)
    async with (
        running_app(memory_config(), clock=clock) as app,
        http_store_over(app, clock=clock) as http_store,
    ):
        yield http_store


@pytest.fixture
async def sqlite_store(tmp_path: Path) -> AsyncIterator[HttpStore]:
    """The same, backed by :class:`~loomwork.store.sqlite.SQLiteStore`."""
    clock = ManualClock(start=T0)
    url = f"sqlite:///{tmp_path}/loomwork.db"
    config = ServerConfig(namespaces={"default": url}, auth=AuthConfig(disabled=True))
    async with (
        running_app(config, clock=clock) as app,
        http_store_over(app, clock=clock) as http_store,
    ):
        yield http_store


def make_tokens_file(path: Path, *tokens: tuple[str, list[str], list[str]]) -> str:
    """Write a tokens file; each ``tokens`` item is ``(id, roles, namespaces)``.

    Returns the plaintext of the *first* token, for convenience in single-token tests.
    """
    from loomwork.server.auth import Role, generate_token

    file = TokenFile()
    plaintexts = []
    for token_id, roles, namespaces in tokens:
        plaintext, record = generate_token(token_id, [Role(r) for r in roles], namespaces)
        file.tokens[record.id] = record
        plaintexts.append(plaintext)
    file.save(path)
    return plaintexts[0]
