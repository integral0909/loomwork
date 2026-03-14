"""The Builds page: drain summary, live workers, and the declare/revoke compatibility
forms -- CSRF, RBAC, escaping."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from loomwork.options import VersioningPolicy
from loomwork.server.state import NamespaceState
from loomwork.store.records import TickOutcome
from tests.server.console.conftest import ConsoleEnv, extract_csrf, login

pytestmark = pytest.mark.integration


async def _login_operator(env: ConsoleEnv) -> None:
    await login(env.client, env.tokens.operator)


async def _pin_a_run(ns: NamespaceState, workflow_id: str, build_id: str) -> str:
    record = await ns.engine.start_workflow(
        "builds.console_noop",
        [],
        workflow_id=workflow_id,
        versioning_policy=VersioningPolicy.PINNED,
    )
    now = datetime.now(UTC)
    claimed = await ns.store.claim_tick("w1", now, timedelta(seconds=30), build_id=build_id)
    assert claimed is not None and claimed.run_id == record.run_id
    outcome = TickOutcome(
        worker_id="w1", consumed_through=0, time=now, outbox=[], build_id=build_id
    )
    await ns.store.complete_tick(record.run_id, "w1", outcome, now)
    return record.run_id


async def test_builds_page_shows_orphaned_build(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await _pin_a_run(ns, "builds-page-1", "build-orphan")
    await _login_operator(console_env)

    response = await console_env.client.get("/console/default/builds")
    assert response.status_code == 200
    assert "build-orphan" in response.text
    assert "orphaned" in response.text


async def test_reader_can_view_but_not_declare_compatibility(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.reader)
    page = await console_env.client.get("/console/default/builds")
    assert page.status_code == 200
    assert "Declare compatibility" not in page.text

    response = await console_env.client.post(
        "/console/default/builds/compatible",
        data={"csrf_token": "irrelevant", "build_id": "build-a", "compatible_with": "build-b"},
    )
    assert response.status_code == 403


async def test_declare_compatibility_drains_an_orphaned_build(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await _pin_a_run(ns, "builds-page-2", "build-old")
    now = datetime.now(UTC)
    await ns.store.heartbeat_worker("worker-new", "build-new", now, now + timedelta(seconds=60))
    await _login_operator(console_env)

    page = await console_env.client.get("/console/default/builds")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/builds/compatible",
        data={"csrf_token": csrf, "build_id": "build-new", "compatible_with": "build-old"},
    )
    assert response.status_code == 303

    followed = await console_env.client.get(response.headers["location"])
    assert "may now claim work" in followed.text

    summary = await ns.engine.versioning.builds_summary()
    old = next(b for b in summary if b.build_id == "build-old")
    assert old.orphaned is False


async def test_declare_compatibility_requires_both_fields(console_env: ConsoleEnv) -> None:
    await _login_operator(console_env)
    page = await console_env.client.get("/console/default/builds")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/builds/compatible",
        data={"csrf_token": csrf, "build_id": "", "compatible_with": "build-old"},
    )
    assert response.status_code == 303
    followed = await console_env.client.get(response.headers["location"])
    assert "Give both" in followed.text


async def test_revoke_compatibility(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await ns.engine.versioning.declare_compatible("build-new", "build-old")
    await _login_operator(console_env)

    page = await console_env.client.get("/console/default/builds")
    csrf = extract_csrf(page.text)
    response = await console_env.client.post(
        "/console/default/builds/build-new/compatible/build-old/revoke",
        data={"csrf_token": csrf},
    )
    assert response.status_code == 303
    assert (await ns.engine.versioning.list_compatibility()) == []


async def test_hostile_build_id_is_escaped(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    hostile = "<script>alert('build')</script>"
    await _pin_a_run(ns, "builds-page-hostile", hostile)
    await _login_operator(console_env)

    response = await console_env.client.get("/console/default/builds")
    assert response.status_code == 200
    assert "<script>alert" not in response.text
    assert "&lt;script&gt;" in response.text


async def test_csrf_wrong_token_rejected(console_env: ConsoleEnv) -> None:
    await _login_operator(console_env)
    response = await console_env.client.post(
        "/console/default/builds/compatible",
        data={"csrf_token": "wrong", "build_id": "build-a", "compatible_with": "build-b"},
    )
    assert response.status_code == 403


async def test_nav_links_to_builds(console_env: ConsoleEnv) -> None:
    await _login_operator(console_env)
    response = await console_env.client.get("/console/default/runs")
    assert '/console/default/builds">Builds</a>' in response.text
