"""Shared fixtures for console tests.

``console_env`` builds a real server (``default`` and ``billing`` namespaces, both
``memory://``) with a token file covering every role, starts a real
:class:`~loomwork.worker.worker.Worker` against ``default`` so runs have genuine
histories (see ``tests/server/test_e2e_worker.py`` and ``tests/server/test_control_api.py``
for the same pattern), and hands back an :class:`httpx.AsyncClient` plus the plaintext
tokens and the registered workflow/step definitions.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from starlette.applications import Starlette

import loomwork as lw
from loomwork.codec.converter import default_converter
from loomwork.engine.engine import Engine
from loomwork.sdk.definitions import Registry
from loomwork.server.app import create_app
from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.records import ExecutionRecord
from loomwork.worker.worker import Worker, WorkerOptions

msg_channel = lw.Channel("msg", str)


@lw.step(name="console.upper")
async def upper(text: str) -> str:
    return text.upper()


@lw.workflow(name="console.child")
async def child_wf(x: int) -> int:
    return x + 1


@lw.workflow(name="console.parent")
async def parent_wf(x: int) -> int:
    return await child_wf(x)


@lw.workflow(name="console.echo")
async def echo_wf(text: str) -> str:
    upped = await upper(text)
    got = await msg_channel.receive()
    return f"{upped}:{got}"


@lw.workflow(name="console.counter")
async def counter_wf(n: int) -> int:
    if n < 2:
        lw.continue_as_new(n + 1)
    return n


@lw.workflow(name="console.boom")
async def boom_wf() -> None:
    raise lw.ApplicationError("kaboom", type="Boom")


REGISTRY = Registry(
    workflows=[child_wf, parent_wf, echo_wf, counter_wf, boom_wf],
    steps=[upper],
)


@dataclass(slots=True)
class Tokens:
    admin: str
    operator: str
    reader: str
    worker: str
    other_namespace_operator: str


@dataclass(slots=True)
class ConsoleEnv:
    app: Starlette
    client: httpx.AsyncClient
    tokens: Tokens


CSRF_RE = re.compile(r'name="csrf_token" value="([^"]+)"')


def extract_csrf(html: str) -> str:
    match = CSRF_RE.search(html)
    assert match, "no csrf_token field found in page"
    return match.group(1)


@pytest.fixture
async def console_env(tmp_path: Path) -> AsyncIterator[ConsoleEnv]:
    tokens_path = tmp_path / "tokens.json"
    admin_pt, admin_rec = generate_token("admin-1", [Role.ADMIN], ["*"])
    operator_pt, operator_rec = generate_token(
        "operator-1", [Role.OPERATOR], ["default", "billing"]
    )
    reader_pt, reader_rec = generate_token("reader-1", [Role.READER], ["default"])
    worker_pt, worker_rec = generate_token("worker-1", [Role.WORKER], ["default"])
    other_pt, other_rec = generate_token("other-ns-1", [Role.OPERATOR], ["billing"])
    TokenFile({r.id: r for r in (admin_rec, operator_rec, reader_rec, worker_rec, other_rec)}).save(
        tokens_path
    )

    config = ServerConfig(
        namespaces={"default": "memory://", "billing": "memory://"},
        auth=AuthConfig(tokens_file=tokens_path),
    )
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        state = app.state.server
        ns = state.namespace("default")
        worker = Worker(ns.store, REGISTRY, options=WorkerOptions(shutdown_grace=1.0))
        async with worker:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                yield ConsoleEnv(
                    app=app,
                    client=client,
                    tokens=Tokens(
                        admin=admin_pt,
                        operator=operator_pt,
                        reader=reader_pt,
                        worker=worker_pt,
                        other_namespace_operator=other_pt,
                    ),
                )


@asynccontextmanager
async def running_app(config: ServerConfig) -> AsyncIterator[tuple[Starlette, httpx.AsyncClient]]:
    """A one-off server for tests that need their own config (a short session TTL, a
    disabled console, a token file with one specific token, ...) rather than the shared
    ``console_env`` fixture's namespaces and tokens."""
    app = create_app(config, run_system_workers=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield app, client


def new_client(env: ConsoleEnv) -> httpx.AsyncClient:
    """A client with its own cookie jar, sharing the app's in-process transport.

    Tests that need two simultaneously logged-in identities (an operator and a reader,
    say) use one of these per identity rather than passing ``cookies=`` per request --
    httpx deprecates that in favour of a client-scoped cookie jar, and this is exactly
    what a separate browser session would look like.
    """
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=env.app), base_url="http://test")


async def login(
    client: httpx.AsyncClient, token: str, *, next_path: str = "/console/"
) -> httpx.Response:
    """Log ``client`` in. Its cookie jar carries the session for later requests."""
    response = await client.post("/console/login", data={"token": token, "next": next_path})
    assert response.status_code == 303, response.text
    return response


async def start_echo(engine: Engine, workflow_id: str, *, text: str = "hi") -> ExecutionRecord:
    """Start ``console.echo`` (runs one step, then waits on the ``msg`` channel)."""
    return await engine.start_workflow(
        "console.echo", [default_converter.encode(text)], workflow_id=workflow_id
    )


async def wait_for_status(
    engine: Engine, run_id: str, status: str, *, timeout: float = 5.0
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        record = await engine.describe(run_id)
        if record.status.value == status:
            return
        if loop.time() > deadline:
            raise AssertionError(f"run {run_id} did not reach {status!r}, still {record.status}")
        await asyncio.sleep(0.02)
