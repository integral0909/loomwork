"""Control API tests: plain JSON in, plain JSON out, no Loomwork Python types involved.

The workflows exercised here run on the server's own internal system worker (registered
directly against the namespace's store via a plain :class:`~loomwork.worker.worker.Worker`
in these fixtures) so a bare ``httpx`` client can drive and observe them exactly the way a
non-Python caller would.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

import loomwork as lw
from loomwork.sdk.definitions import Registry
from loomwork.server.app import create_app
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.factory import store_from_url
from loomwork.worker.worker import Worker, WorkerOptions

greetings = lw.Channel("greet", str)


@lw.step(name="control.upper")
async def upper(text: str) -> str:
    return text.upper()


@lw.workflow(name="control.echo")
async def echo(text: str) -> str:
    return await upper(text)


@lw.workflow(name="control.waits_for_signal")
async def waits_for_signal() -> str:
    return await greetings.receive()


@lw.workflow(name="control.has_update")
async def has_update() -> int:
    total = 0

    @lw.update("add")
    def add(n: int) -> int:
        nonlocal total
        total += n
        return total

    await lw.sleep(3600)  # stay open until cancelled/terminated by the tests below
    return total


REGISTRY = Registry(workflows=[echo, waits_for_signal, has_update], steps=[upper])


@pytest.fixture
async def control_client(tmp_path: Path) -> AsyncIterator[httpx.AsyncClient]:
    store_url = f"sqlite:///{tmp_path}/control.db"
    config = ServerConfig(namespaces={"default": store_url}, auth=AuthConfig(disabled=True))
    app = create_app(config, run_system_workers=True)
    async with app.router.lifespan_context(app):
        # A real worker executing this namespace's workflows, so control-API calls have
        # something to observe (start, signal, update, cancel, terminate all need code
        # actually running the ticks).
        store = store_from_url(store_url)
        await store.open()
        worker = Worker(store, REGISTRY, options=WorkerOptions(shutdown_grace=1.0))
        async with worker:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://test/v1/namespaces/default/control",
            ) as client:
                yield client
        await store.close()


async def _wait_status(
    client: httpx.AsyncClient, run_id: str, status: str, timeout: float = 5.0
) -> dict[str, Any]:
    import asyncio

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        response = await client.get(f"/workflows/{run_id}")
        body: dict[str, Any] = response.json()
        if body["status"] == status:
            return body
        if loop.time() > deadline:
            raise AssertionError(f"run {run_id} did not reach {status!r}: {body}")
        await asyncio.sleep(0.02)


async def test_start_and_result(control_client: httpx.AsyncClient) -> None:
    response = await control_client.post(
        "/workflows", json={"name": "control.echo", "args": ["hi"], "workflow_id": "echo-1"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["workflow_id"] == "echo-1"
    run_id = body["run_id"]

    result = await control_client.get(f"/workflows/{run_id}/result", params={"timeout": 5})
    assert result.status_code == 200
    assert result.json()["status"] == "completed"
    assert result.json()["result"] == "HI"


async def test_list_and_history(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post("/workflows", json={"name": "control.echo", "args": ["a"]})
    run_id = started.json()["run_id"]
    await _wait_status(control_client, run_id, "completed")

    listed = await control_client.get("/workflows", params={"workflow_name": "control.echo"})
    assert listed.status_code == 200
    assert any(r["run_id"] == run_id for r in listed.json()["executions"])

    history = await control_client.get(f"/workflows/{run_id}/history")
    assert history.status_code == 200
    kinds = [e["kind"] for e in history.json()["events"]]
    assert kinds[0] == "workflow_started"
    assert "workflow_completed" in kinds


async def test_signal(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post(
        "/workflows", json={"name": "control.waits_for_signal", "workflow_id": "wfs-1"}
    )
    run_id = started.json()["run_id"]
    signalled = await control_client.post(
        f"/workflows/{run_id}/signal", json={"channel": "greet", "value": "hello"}
    )
    assert signalled.status_code == 204
    result = await control_client.get(f"/workflows/{run_id}/result", params={"timeout": 5})
    assert result.json()["result"] == "hello"


async def test_signal_with_start(control_client: httpx.AsyncClient) -> None:
    response = await control_client.post(
        "/workflows/signal-with-start",
        json={
            "name": "control.waits_for_signal",
            "workflow_id": "swf-1",
            "channel": "greet",
            "value": "hi-swf",
        },
    )
    assert response.status_code == 201
    run_id = response.json()["run_id"]
    result = await _wait_status(control_client, run_id, "completed")
    assert result["result"] == "hi-swf"


async def test_request_and_wait_update(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post("/workflows", json={"name": "control.has_update"})
    run_id = started.json()["run_id"]
    requested = await control_client.post(
        f"/workflows/{run_id}/updates", json={"name": "add", "args": [4]}
    )
    assert requested.status_code == 202
    update_id = requested.json()["update_id"]
    outcome = await control_client.get(
        f"/workflows/{run_id}/updates/{update_id}", params={"timeout": 5}
    )
    assert outcome.status_code == 200
    assert outcome.json() == {"update_id": update_id, "status": "completed", "result": 4}
    await control_client.post(f"/workflows/{run_id}/terminate", json={"reason": "done"})


async def test_cancel_and_terminate(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post("/workflows", json={"name": "control.has_update"})
    run_id = started.json()["run_id"]
    cancelled = await control_client.post(f"/workflows/{run_id}/cancel", json={"reason": "x"})
    assert cancelled.status_code == 200
    assert cancelled.json()["ok"] is True

    terminated = await control_client.post(
        f"/workflows/{run_id}/terminate", json={"reason": "stop"}
    )
    assert terminated.status_code == 200
    result = await _wait_status(control_client, run_id, "terminated")
    assert result["close_reason"] == "stop"
    # Idempotent: terminating an already-closed run reports False, not an error.
    again = await control_client.post(f"/workflows/{run_id}/terminate", json={"reason": "stop"})
    assert again.json()["ok"] is False


async def test_children_endpoint(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post(
        "/workflows", json={"name": "control.echo", "args": ["solo"], "workflow_id": "no-children"}
    )
    run_id = started.json()["run_id"]
    await _wait_status(control_client, run_id, "completed")
    children = await control_client.get(f"/workflows/{run_id}/children")
    assert children.status_code == 200
    assert children.json() == {"children": []}


async def test_validation_errors_are_400(control_client: httpx.AsyncClient) -> None:
    response = await control_client.post("/workflows", json={"args": ["missing name"]})
    assert response.status_code == 400
    assert response.json()["code"] == "invalid_request"


async def test_unknown_run_is_404(control_client: httpx.AsyncClient) -> None:
    response = await control_client.get("/workflows/does-not-exist")
    assert response.status_code == 404
    assert response.json()["code"] == "not_found"


async def test_duplicate_workflow_id_is_409(control_client: httpx.AsyncClient) -> None:
    first = await control_client.post(
        "/workflows", json={"name": "control.has_update", "workflow_id": "dup-1"}
    )
    assert first.status_code == 201
    second = await control_client.post(
        "/workflows", json={"name": "control.has_update", "workflow_id": "dup-1"}
    )
    assert second.status_code == 409
    assert second.json()["code"] == "workflow_already_started"


async def test_start_with_use_existing_conflict_policy(control_client: httpx.AsyncClient) -> None:
    first = await control_client.post(
        "/workflows", json={"name": "control.has_update", "workflow_id": "use-existing-1"}
    )
    assert first.status_code == 201
    second = await control_client.post(
        "/workflows",
        json={
            "name": "control.has_update",
            "workflow_id": "use-existing-1",
            "id_conflict_policy": "use_existing",
        },
    )
    assert second.status_code == 201
    assert second.json()["run_id"] == first.json()["run_id"]


async def test_start_with_reject_duplicate_reuse_policy(
    control_client: httpx.AsyncClient,
) -> None:
    started = await control_client.post(
        "/workflows",
        json={"name": "control.echo", "args": ["a"], "workflow_id": "reject-dup-1"},
    )
    run_id = started.json()["run_id"]
    await _wait_status(control_client, run_id, "completed")

    rejected = await control_client.post(
        "/workflows",
        json={
            "name": "control.echo",
            "args": ["b"],
            "workflow_id": "reject-dup-1",
            "id_reuse_policy": "reject_duplicate",
        },
    )
    assert rejected.status_code == 409
    assert rejected.json()["code"] == "workflow_id_reuse_rejected"


async def test_reset_workflow(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post(
        "/workflows", json={"name": "control.waits_for_signal", "workflow_id": "reset-1"}
    )
    run_id = started.json()["run_id"]

    reset = await control_client.post(
        f"/workflows/{run_id}/reset",
        json={"to": "first_tick", "reason": "operator reset"},
    )
    assert reset.status_code == 201
    new_run_id = reset.json()["run_id"]
    assert new_run_id != run_id

    old = await control_client.get(f"/workflows/{run_id}")
    assert old.json()["status"] == "terminated"
    assert old.json()["continued_run_id"] == new_run_id

    reply = await control_client.post(
        f"/workflows/{new_run_id}/signal", json={"channel": "greet", "value": "hi"}
    )
    assert reply.status_code == 204
    result = await control_client.get(f"/workflows/{new_run_id}/result", params={"timeout": 5})
    assert result.json()["result"] == "hi"


async def test_reset_requires_exactly_one_of_to_or_after_tick(
    control_client: httpx.AsyncClient,
) -> None:
    started = await control_client.post(
        "/workflows", json={"name": "control.waits_for_signal", "workflow_id": "reset-bad"}
    )
    run_id = started.json()["run_id"]
    response = await control_client.post(f"/workflows/{run_id}/reset", json={"reason": "x"})
    assert response.status_code == 400
