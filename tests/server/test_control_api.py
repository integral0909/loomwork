"""Control API tests: plain JSON in, plain JSON out, no Loomwork Python types involved.

The workflows exercised here run on the server's own internal system worker (registered
directly against the namespace's store via a plain :class:`~loomwork.worker.worker.Worker`
in these fixtures) so a bare ``httpx`` client can drive and observe them exactly the way a
non-Python caller would.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
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


async def test_whoami(control_client: httpx.AsyncClient) -> None:
    # auth.disabled: every request is an anonymous administrator.
    response = await control_client.get("/whoami")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "anonymous"
    assert body["roles"] == ["admin"]
    assert body["namespaces"] == ["*"]
    datetime.fromisoformat(body["server_time"])  # does not raise


async def test_describe_by_workflow_id_resolves_the_latest_run(
    control_client: httpx.AsyncClient,
) -> None:
    started = await control_client.post(
        "/workflows", json={"name": "control.echo", "args": ["a"], "workflow_id": "by-id-1"}
    )
    run_id = started.json()["run_id"]
    response = await control_client.get("/workflows/by-id/by-id-1")
    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert body["search_attributes"] == {}


async def test_describe_includes_search_attributes(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post(
        "/workflows",
        json={
            "name": "control.waits_for_signal",
            "workflow_id": "sa-1",
            "search_attributes": {},
        },
    )
    run_id = started.json()["run_id"]
    response = await control_client.get(f"/workflows/{run_id}")
    assert response.status_code == 200
    assert "search_attributes" in response.json()


async def test_start_with_timeouts_and_retry(control_client: httpx.AsyncClient) -> None:
    response = await control_client.post(
        "/workflows",
        json={
            "name": "control.waits_for_signal",
            "workflow_id": "opts-1",
            "run_timeout": 3600,
            "execution_timeout": 7200,
            "retry_max_attempts": 3,
        },
    )
    assert response.status_code == 201
    run_id = response.json()["run_id"]
    history = await control_client.get(f"/workflows/{run_id}/history")
    started_event = history.json()["events"][0]
    assert started_event["run_timeout"] == 3600
    assert started_event["retry_policy"]["max_attempts"] == 3


async def test_list_with_visibility_query(control_client: httpx.AsyncClient) -> None:
    started = await control_client.post(
        "/workflows", json={"name": "control.echo", "args": ["q"], "workflow_id": "query-list-1"}
    )
    run_id = started.json()["run_id"]
    await _wait_status(control_client, run_id, "completed")

    response = await control_client.get(
        "/workflows", params={"query": "workflow_id = 'query-list-1'"}
    )
    assert response.status_code == 200
    ids = [e["workflow_id"] for e in response.json()["executions"]]
    assert ids == ["query-list-1"]


async def test_list_query_cannot_combine_with_filters(control_client: httpx.AsyncClient) -> None:
    response = await control_client.get(
        "/workflows", params={"query": "status = 'running'", "status": "running"}
    )
    assert response.status_code == 400


# --------------------------------------------------------------------------- schedules


async def test_schedule_lifecycle(control_client: httpx.AsyncClient) -> None:
    created = await control_client.post(
        "/schedules",
        json={
            "schedule_id": "sched-1",
            "workflow": "control.echo",
            "args": ["hi"],
            "cron": "0 3 * * *",
        },
    )
    assert created.status_code == 201
    body = created.json()
    assert body["schedule_id"] == "sched-1"
    assert body["action"]["args"] == ["hi"]
    assert body["paused"] is False

    listed = await control_client.get("/schedules")
    assert listed.status_code == 200
    assert any(s["schedule_id"] == "sched-1" for s in listed.json()["schedules"])

    described = await control_client.get("/schedules/sched-1")
    assert described.status_code == 200
    assert described.json()["spec"]["cron"] == "0 3 * * *"

    upcoming = await control_client.get("/schedules/sched-1/upcoming", params={"count": 3})
    assert upcoming.status_code == 200
    assert len(upcoming.json()["upcoming"]) == 3

    paused = await control_client.post("/schedules/sched-1/pause", json={"note": "quiet time"})
    assert paused.status_code == 200
    assert paused.json()["paused"] is True
    assert paused.json()["note"] == "quiet time"

    unpaused = await control_client.post("/schedules/sched-1/unpause", json={})
    assert unpaused.status_code == 200
    assert unpaused.json()["paused"] is False

    updated = await control_client.post(
        "/schedules/sched-1/update", json={"cron": "0 4 * * *", "pause_on_failure": True}
    )
    assert updated.status_code == 200
    assert updated.json()["spec"]["cron"] == "0 4 * * *"
    assert updated.json()["policy"]["pause_on_failure"] is True

    triggered = await control_client.post("/schedules/sched-1/trigger")
    assert triggered.status_code == 200
    assert triggered.json()["total_started"] == 1

    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    backfilled = await control_client.post(
        "/schedules/sched-1/backfill",
        json={"start": (now - timedelta(days=2)).isoformat(), "end": now.isoformat()},
    )
    assert backfilled.status_code == 200
    assert backfilled.json()["total_started"] >= 1

    deleted = await control_client.delete("/schedules/sched-1")
    assert deleted.status_code == 200
    assert deleted.json()["ok"] is True

    gone = await control_client.get("/schedules/sched-1")
    assert gone.status_code == 404


async def test_create_schedule_requires_exactly_one_of_cron_and_interval(
    control_client: httpx.AsyncClient,
) -> None:
    response = await control_client.post(
        "/schedules",
        json={"schedule_id": "bad-sched", "workflow": "control.echo", "args": []},
    )
    assert response.status_code == 400

    response = await control_client.post(
        "/schedules",
        json={
            "schedule_id": "bad-sched",
            "workflow": "control.echo",
            "args": [],
            "cron": "0 3 * * *",
            "interval": 60,
        },
    )
    assert response.status_code == 400


async def test_update_schedule_with_no_changes_is_400(control_client: httpx.AsyncClient) -> None:
    await control_client.post(
        "/schedules",
        json={"schedule_id": "noop-sched", "workflow": "control.echo", "args": [], "interval": 60},
    )
    response = await control_client.post("/schedules/noop-sched/update", json={})
    assert response.status_code == 400


async def test_delete_unknown_schedule_reports_false(control_client: httpx.AsyncClient) -> None:
    response = await control_client.delete("/schedules/does-not-exist")
    assert response.status_code == 200
    assert response.json()["ok"] is False


# --------------------------------------------------------------------------- RBAC


@pytest.fixture
async def rbac_app() -> AsyncIterator[tuple[httpx.AsyncClient, str, str]]:
    """A server with a real token file: one ``operator`` token, one ``reader`` token."""
    from loomwork.server.auth import Role, TokenFile, generate_token

    operator_token, operator_record = generate_token("op-1", [Role.OPERATOR], ["default"])
    reader_token, reader_record = generate_token("rd-1", [Role.READER], ["default"])
    tokens = TokenFile({operator_record.id: operator_record, reader_record.id: reader_record})
    import tempfile

    tmp_dir = Path(tempfile.mkdtemp())
    tokens_path = tmp_dir / "tokens.json"
    tokens.save(tokens_path)
    config = ServerConfig(
        namespaces={"default": "memory://"}, auth=AuthConfig(tokens_file=tokens_path)
    )
    app = create_app(config, run_system_workers=True)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test/v1/namespaces/default/control"
        ) as client:
            yield client, operator_token, reader_token


async def test_whoami_reports_the_tokens_roles(
    rbac_app: tuple[httpx.AsyncClient, str, str],
) -> None:
    client, operator_token, reader_token = rbac_app
    op_response = await client.get("/whoami", headers={"Authorization": f"Bearer {operator_token}"})
    assert op_response.status_code == 200
    assert op_response.json()["roles"] == ["operator"]
    assert op_response.json()["namespaces"] == ["default"]

    reader_response = await client.get(
        "/whoami", headers={"Authorization": f"Bearer {reader_token}"}
    )
    assert reader_response.json()["roles"] == ["reader"]

    unauthenticated = await client.get("/whoami")
    assert unauthenticated.status_code == 401


async def test_reader_can_list_schedules_but_not_create(
    rbac_app: tuple[httpx.AsyncClient, str, str],
) -> None:
    client, operator_token, reader_token = rbac_app
    created = await client.post(
        "/schedules",
        json={"schedule_id": "rbac-sched", "workflow": "wf.x", "args": [], "interval": 60},
        headers={"Authorization": f"Bearer {operator_token}"},
    )
    assert created.status_code == 201

    listed = await client.get("/schedules", headers={"Authorization": f"Bearer {reader_token}"})
    assert listed.status_code == 200

    forbidden = await client.post(
        "/schedules/rbac-sched/pause",
        json={},
        headers={"Authorization": f"Bearer {reader_token}"},
    )
    assert forbidden.status_code == 403


# --------------------------------------------------------------------------- search attributes


async def test_query_can_reference_declared_search_attributes() -> None:
    """``ServerConfig.search_attributes`` reaches the namespace's ``Engine``, so a
    visibility query naming a declared attribute works through the control API's
    ``?query=`` -- and one naming an *undeclared* attribute still fails, exactly as it
    would locally with an empty schema."""
    config = ServerConfig(
        namespaces={"default": "memory://"},
        auth=AuthConfig(disabled=True),
        search_attributes={"default": {"tier": "string"}},
    )
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test/v1/namespaces/default/control"
        ) as client:
            started = await client.post(
                "/workflows",
                json={
                    "name": "control.echo",
                    "args": ["a"],
                    "workflow_id": "sa-query-1",
                    "search_attributes": {"tier": "gold"},
                },
            )
            assert started.status_code == 201

            declared = await client.get("/workflows", params={"query": "attr.tier = 'gold'"})
            assert declared.status_code == 200
            assert [e["workflow_id"] for e in declared.json()["executions"]] == ["sa-query-1"]

            undeclared = await client.get("/workflows", params={"query": "attr.region = 'eu'"})
            assert undeclared.status_code == 400
            assert undeclared.json()["code"] == "query_type_error"
