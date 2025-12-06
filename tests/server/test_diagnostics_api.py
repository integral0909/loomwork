"""``GET /control/diagnostics``: the aggregate health facts behind ``loomwork doctor
--server`` (see ``loomwork.diagnostics``).

No system worker runs in these fixtures, so a condition created directly against the
namespace's store (a failed tick claim) stays exactly as constructed until the assertion
that checks for it -- nothing races to fix it first.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from loomwork.engine.engine import Engine
from loomwork.server.app import create_app
from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.factory import store_from_url

pytestmark = pytest.mark.integration


@pytest.fixture
async def diagnostics_client(
    tmp_path: Path,
) -> AsyncIterator[tuple[httpx.AsyncClient, dict[str, str]]]:
    tokens_path = tmp_path / "tokens.json"
    file = TokenFile()
    plaintexts: dict[str, str] = {}
    for name, role in (
        ("operator", Role.OPERATOR),
        ("reader", Role.READER),
        ("worker", Role.WORKER),
    ):
        plaintext, record = generate_token(name, [role], ["default"])
        file.tokens[record.id] = record
        plaintexts[name] = plaintext
    file.save(tokens_path)

    store_url = f"sqlite:///{tmp_path}/diagnostics.db"
    config = ServerConfig(
        namespaces={"default": store_url}, auth=AuthConfig(tokens_file=tokens_path)
    )
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test/v1/namespaces/default/control"
        ) as client:
            yield client, plaintexts


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def test_diagnostics_shape_on_a_healthy_store(
    diagnostics_client: tuple[httpx.AsyncClient, dict[str, str]],
) -> None:
    client, tokens = diagnostics_client
    response = await client.get("/diagnostics", headers=_auth(tokens["operator"]))
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == body["latest_known_schema_version"]
    assert body["outbox_backlog"] == 0
    assert body["tick_failing"] == []
    assert body["queue_backlogs"] == [{"queue": None, "oldest_ready_at": None}]
    assert body["schedules_overdue"] == []
    assert body["large_histories"] == []
    datetime.fromisoformat(body["now"])


async def test_diagnostics_reports_a_real_tick_failure(
    diagnostics_client: tuple[httpx.AsyncClient, dict[str, str]], tmp_path: Path
) -> None:
    client, tokens = diagnostics_client
    store = store_from_url(f"sqlite:///{tmp_path}/diagnostics.db")
    await store.open()
    try:
        engine = Engine(store)
        record = await engine.start_workflow("doctor.stuck", [], workflow_id="api-stuck-1")
        now = datetime.now(UTC)
        claimed = await store.claim_tick("w1", now, timedelta(seconds=30))
        assert claimed is not None and claimed.run_id == record.run_id
        await store.fail_tick(record.run_id, "w1", "boom", now, now)
    finally:
        await store.close()

    response = await client.get(
        "/diagnostics",
        params={"tick_failure_threshold": 1},
        headers=_auth(tokens["operator"]),
    )
    assert response.status_code == 200
    failing = response.json()["tick_failing"]
    assert len(failing) == 1
    assert failing[0]["workflow_id"] == "api-stuck-1"
    assert failing[0]["last_tick_error"] == "boom"


async def test_diagnostics_bounds_and_filters_by_queue(
    diagnostics_client: tuple[httpx.AsyncClient, dict[str, str]],
) -> None:
    client, tokens = diagnostics_client
    response = await client.get(
        "/diagnostics",
        params={"queue": ["a", "b"], "limit": 5},
        headers=_auth(tokens["operator"]),
    )
    assert response.status_code == 200
    backlogs = {b["queue"]: b for b in response.json()["queue_backlogs"]}
    assert set(backlogs) == {None, "a", "b"}


async def test_reader_can_read_diagnostics(
    diagnostics_client: tuple[httpx.AsyncClient, dict[str, str]],
) -> None:
    client, tokens = diagnostics_client
    response = await client.get("/diagnostics", headers=_auth(tokens["reader"]))
    assert response.status_code == 200


async def test_worker_token_cannot_read_diagnostics(
    diagnostics_client: tuple[httpx.AsyncClient, dict[str, str]],
) -> None:
    client, tokens = diagnostics_client
    response = await client.get("/diagnostics", headers=_auth(tokens["worker"]))
    assert response.status_code == 403


async def test_unauthenticated_diagnostics_is_401(
    diagnostics_client: tuple[httpx.AsyncClient, dict[str, str]],
) -> None:
    client, _tokens = diagnostics_client
    response = await client.get("/diagnostics")
    assert response.status_code == 401


async def test_whoami_carries_namespaces_and_server_time(
    diagnostics_client: tuple[httpx.AsyncClient, dict[str, str]],
) -> None:
    client, tokens = diagnostics_client
    response = await client.get("/whoami", headers=_auth(tokens["operator"]))
    body = response.json()
    assert body["namespaces"] == ["default"]
    datetime.fromisoformat(body["server_time"])
