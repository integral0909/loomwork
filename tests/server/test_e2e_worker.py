"""End-to-end: a remote worker and a remote client, both over ``HttpStore``, running a
workflow with steps, a durable timer, a signal, an update and a child workflow against
an in-process server (no real socket).

This exercises the full stack together: token authentication and RBAC, the worker API
driving ``Worker``'s tick/step/timer loops and ``Engine``/``Client``'s control calls
unmodified (both go through ``HttpStore``, i.e. the worker API -- the plain-JSON control
API is a separate surface for callers without Loomwork's Python types), and that a
worker-only token cannot reach the control API and an operator-only token cannot reach
the worker API.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest

import loomwork as lw
from loomwork.client.client import Client
from loomwork.engine.config import EngineConfig
from loomwork.engine.engine import Engine
from loomwork.remote.http_store import HttpStore
from loomwork.retry import RetryPolicy
from loomwork.sdk.definitions import Registry
from loomwork.server.app import create_app
from loomwork.server.auth import Role, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.worker.worker import Worker, WorkerOptions
from tests.helpers import eventually

pytestmark = pytest.mark.integration

FAST_CONFIG = EngineConfig(
    poll_interval=0.02,
    tick_retry=RetryPolicy(initial_interval=0.01, max_interval=0.05, max_attempts=0, jitter=0),
    default_step_retry=RetryPolicy(initial_interval=0.01, max_interval=0.05, jitter=0),
)

progress = lw.Channel("progress", str)


@lw.step(name="e2e.add")
async def add(a: int, b: int) -> int:
    return a + b


@lw.workflow(name="e2e.child")
async def child_wf(x: int) -> int:
    return await add(x, 1)


@lw.workflow(name="e2e.main")
async def main_wf(start: int) -> dict[str, Any]:
    total = await add(start, 10)
    await lw.sleep(0.01)
    child_result = await child_wf(total)

    counter = {"bumps": 0}

    @lw.update("bump")
    def bump(amount: int) -> int:
        counter["bumps"] += amount
        return counter["bumps"]

    message = await progress.receive()
    return {
        "total": total,
        "child": child_result,
        "bumps": counter["bumps"],
        "message": message,
    }


REGISTRY = Registry(workflows=[main_wf, child_wf], steps=[add])


@pytest.fixture
async def server_app(tmp_path: Path):
    tokens_path = tmp_path / "tokens.json"
    # Python's Engine/Client and Worker both operate through HttpStore, i.e. the worker
    # API -- the control API is a separate, plain-JSON surface for non-Python callers
    # (see tests below). Both connections here therefore need the "worker" capability.
    worker_token, worker_record = generate_token("worker-1", [Role.WORKER], ["default"])
    client_token, client_record = generate_token("client-1", [Role.WORKER], ["default"])
    operator_token, operator_record = generate_token("operator-1", [Role.OPERATOR], ["default"])
    from loomwork.server.auth import TokenFile

    tokens = TokenFile(
        {
            worker_record.id: worker_record,
            client_record.id: client_record,
            operator_record.id: operator_record,
        }
    )
    tokens.save(tokens_path)

    config = ServerConfig(
        namespaces={"default": "memory://"}, auth=AuthConfig(tokens_file=tokens_path)
    )
    app = create_app(config, run_system_workers=True)
    async with app.router.lifespan_context(app):
        yield app, worker_token, client_token, operator_token


async def test_remote_worker_and_client(server_app: Any) -> None:
    app, worker_token, client_token, operator_token = server_app
    transport = httpx.ASGITransport(app=app)

    worker_store = HttpStore(
        "http://test", worker_token, "default", transport=transport, long_poll=0.05
    )
    client_store = HttpStore(
        "http://test", client_token, "default", transport=transport, long_poll=0.0
    )
    await worker_store.open()
    await client_store.open()
    try:
        worker = Worker(
            worker_store,
            REGISTRY,
            config=FAST_CONFIG,
            options=WorkerOptions(workflow_concurrency=2, step_concurrency=4),
        )
        engine = Engine(client_store, config=FAST_CONFIG)
        client = Client(engine, registry=REGISTRY)

        async with worker:
            handle = await client.start(main_wf, 5, id="e2e-main-1")

            # The workflow is blocked on progress.receive(); poke it with an update and
            # a signal, both routed through the control-plane Engine over HttpStore.
            await eventually(lambda: _has_pending_update_target(client, handle))
            assert await handle.update("bump", 3) == 3
            assert await handle.update("bump", 4) == 7

            await handle.signal(progress, "go")
            result = await handle.result(timeout=10)

        assert result == {"total": 15, "child": 16, "bumps": 7, "message": "go"}

        # The control API (plain JSON, a separate surface for non-Python callers) sees
        # the same run, through a token with the operator role instead.
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
            response = await http.get(
                f"/v1/namespaces/default/control/workflows/{handle.run_id}",
                headers={"Authorization": f"Bearer {operator_token}"},
            )
        assert response.status_code == 200
        assert response.json()["status"] == "completed"
    finally:
        await worker_store.close()
        await client_store.close()


async def _has_pending_update_target(client: Client, handle: Any) -> bool:
    # The workflow needs to have reached its update-registering point before an update
    # is guaranteed to be handled promptly; polling history is a simple, robust proxy.
    history = await handle.history()
    return any(e.kind == "tick_completed" for e in history)


async def test_worker_token_cannot_use_control_api(server_app: Any) -> None:
    app, worker_token, _client_token, _operator_token = server_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.post(
            "/v1/namespaces/default/control/workflows",
            json={"name": "e2e.main", "args": [1]},
            headers={"Authorization": f"Bearer {worker_token}"},
        )
        assert response.status_code == 403


async def test_operator_token_cannot_use_worker_api(server_app: Any) -> None:
    app, _worker_token, _client_token, operator_token = server_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.post(
            "/v1/namespaces/default/ticks/claim",
            json={"worker_id": "x", "lease_seconds": 1, "wait": 0},
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert response.status_code == 403
