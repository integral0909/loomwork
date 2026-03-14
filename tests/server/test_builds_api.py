"""``/control/builds...`` and ``/control/workflows/{run_id}/repin``: worker build
versioning over the control API (see ``docs/adr/0006-worker-build-versioning.md``).

Pinned runs and worker heartbeats are constructed directly against a second connection
to the same SQLite file (the same technique ``test_diagnostics_api.py`` uses), since the
server's own store instance is what the control API calls answer from.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from loomwork.engine.engine import Engine
from loomwork.options import VersioningPolicy
from loomwork.server.app import create_app
from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.factory import store_from_url
from loomwork.store.records import TickOutcome

pytestmark = pytest.mark.integration


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def builds_client(
    tmp_path: Path,
) -> AsyncIterator[tuple[httpx.AsyncClient, dict[str, str], Path]]:
    tokens_path = tmp_path / "tokens.json"
    file = TokenFile()
    plaintexts: dict[str, str] = {}
    for name, role in (("operator", Role.OPERATOR), ("reader", Role.READER)):
        plaintext, record = generate_token(name, [role], ["default"])
        file.tokens[record.id] = record
        plaintexts[name] = plaintext
    file.save(tokens_path)

    db_path = tmp_path / "builds.db"
    store_url = f"sqlite:///{db_path}"
    config = ServerConfig(
        namespaces={"default": store_url}, auth=AuthConfig(tokens_file=tokens_path)
    )
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test/v1/namespaces/default/control"
        ) as client:
            yield client, plaintexts, db_path


async def _pin_a_run(db_path: Path, workflow_id: str, build_id: str) -> str:
    store = store_from_url(f"sqlite:///{db_path}")
    await store.open()
    try:
        engine = Engine(store)
        record = await engine.start_workflow(
            "builds.noop", [], workflow_id=workflow_id, versioning_policy=VersioningPolicy.PINNED
        )
        now = datetime.now(UTC)
        claimed = await store.claim_tick("w1", now, timedelta(seconds=30), build_id=build_id)
        assert claimed is not None and claimed.run_id == record.run_id
        outcome = TickOutcome(
            worker_id="w1", consumed_through=0, time=now, outbox=[], build_id=build_id
        )
        await store.complete_tick(record.run_id, "w1", outcome, now)
        return record.run_id
    finally:
        await store.close()


async def _heartbeat(db_path: Path, worker_id: str, build_id: str) -> None:
    store = store_from_url(f"sqlite:///{db_path}")
    await store.open()
    try:
        now = datetime.now(UTC)
        await store.heartbeat_worker(worker_id, build_id, now, now + timedelta(seconds=60))
    finally:
        await store.close()


async def test_builds_summary_and_workers(
    builds_client: tuple[httpx.AsyncClient, dict[str, str], Path],
) -> None:
    client, tokens, db_path = builds_client
    await _pin_a_run(db_path, "pinned-1", "build-orphan")
    await _heartbeat(db_path, "worker-1", "build-live")

    response = await client.get("/builds", headers=_auth(tokens["operator"]))
    assert response.status_code == 200
    builds = {b["build_id"]: b for b in response.json()["builds"]}
    assert builds["build-orphan"]["pinned_running_runs"] == 1
    assert builds["build-orphan"]["orphaned"] is True
    assert builds["build-live"]["live_workers"] == 1
    assert builds["build-live"]["orphaned"] is False

    workers = await client.get("/builds/workers", headers=_auth(tokens["operator"]))
    assert workers.status_code == 200
    assert {w["worker_id"]: w["build_id"] for w in workers.json()["workers"]} == {
        "worker-1": "build-live"
    }


async def test_declare_and_revoke_compatibility(
    builds_client: tuple[httpx.AsyncClient, dict[str, str], Path],
) -> None:
    client, tokens, db_path = builds_client
    await _pin_a_run(db_path, "pinned-2", "build-old")

    declared = await client.post(
        "/builds/build-new/compatible",
        json={"compatible_with": "build-old"},
        headers=_auth(tokens["operator"]),
    )
    assert declared.status_code == 200

    listed = await client.get("/builds/compatibility", headers=_auth(tokens["operator"]))
    compatibility = listed.json()["compatibility"]
    assert len(compatibility) == 1
    assert compatibility[0]["build_id"] == "build-new"
    assert compatibility[0]["compatible_with"] == "build-old"
    datetime.fromisoformat(compatibility[0]["created_at"])

    revoked = await client.delete(
        "/builds/build-new/compatible/build-old", headers=_auth(tokens["operator"])
    )
    assert revoked.status_code == 200
    assert revoked.json()["ok"] is True

    revoked_again = await client.delete(
        "/builds/build-new/compatible/build-old", headers=_auth(tokens["operator"])
    )
    assert revoked_again.json()["ok"] is False


async def test_reader_can_read_builds_but_not_declare_compatible(
    builds_client: tuple[httpx.AsyncClient, dict[str, str], Path],
) -> None:
    client, tokens, _db_path = builds_client
    ok = await client.get("/builds", headers=_auth(tokens["reader"]))
    assert ok.status_code == 200

    forbidden = await client.post(
        "/builds/build-new/compatible",
        json={"compatible_with": "build-old"},
        headers=_auth(tokens["reader"]),
    )
    assert forbidden.status_code == 403


async def test_repin_moves_a_pinned_run(
    builds_client: tuple[httpx.AsyncClient, dict[str, str], Path],
) -> None:
    client, tokens, db_path = builds_client
    run_id = await _pin_a_run(db_path, "pinned-3", "build-old")

    response = await client.post(
        f"/workflows/{run_id}/repin",
        json={"build_id": "build-new"},
        headers=_auth(tokens["operator"]),
    )
    assert response.status_code == 200
    assert response.json()["pinned_build_id"] == "build-new"

    unpinned = await client.post(
        f"/workflows/{run_id}/repin", json={"build_id": None}, headers=_auth(tokens["operator"])
    )
    assert unpinned.json()["pinned_build_id"] is None


async def test_repin_unknown_run_is_404(
    builds_client: tuple[httpx.AsyncClient, dict[str, str], Path],
) -> None:
    client, tokens, _db_path = builds_client
    response = await client.post(
        "/workflows/ghost/repin", json={"build_id": "build-a"}, headers=_auth(tokens["operator"])
    )
    assert response.status_code == 404


async def test_reader_cannot_repin(
    builds_client: tuple[httpx.AsyncClient, dict[str, str], Path],
) -> None:
    client, tokens, db_path = builds_client
    run_id = await _pin_a_run(db_path, "pinned-4", "build-old")
    response = await client.post(
        f"/workflows/{run_id}/repin",
        json={"build_id": "build-new"},
        headers=_auth(tokens["reader"]),
    )
    assert response.status_code == 403
