"""The diagnostics page: same report as ``loomwork doctor``, RBAC, nav link."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.server.console.conftest import ConsoleEnv, login

pytestmark = pytest.mark.integration


async def test_diagnostics_page_renders_for_a_healthy_namespace(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/diagnostics")
    assert response.status_code == 200
    assert "Diagnostics" in response.text
    assert "outbox backlog" in response.text
    assert "PASS" in response.text
    assert "FAIL" not in response.text


async def test_reader_can_view_diagnostics(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.get("/console/default/diagnostics")
    assert response.status_code == 200


async def test_worker_cannot_view_diagnostics(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.worker)
    response = await console_env.client.get("/console/default/diagnostics")
    assert response.status_code == 403


async def test_other_namespace_token_cannot_view_diagnostics(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.other_namespace_operator)
    response = await console_env.client.get("/console/default/diagnostics")
    assert response.status_code == 403


async def test_nav_links_to_diagnostics(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/runs")
    assert '/console/default/diagnostics">Diagnostics</a>' in response.text


async def test_diagnostics_reports_a_real_tick_failure(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    # A workflow name the fixture's worker does not register, so its tick-claiming loop
    # (which only claims registered workflows, see TickProcessor.claim) never races this
    # test's own manual claim below.
    record = await ns.engine.start_workflow("diag.unregistered", [], workflow_id="diag-stuck-1")
    now = datetime.now(UTC)
    # The default threshold (both here and in `loomwork doctor`) is 3 consecutive
    # failures; retry_at=now keeps each one immediately reclaimable.
    for _ in range(3):
        claimed = await ns.store.claim_tick("w1", now, timedelta(seconds=30))
        assert claimed is not None and claimed.run_id == record.run_id
        await ns.store.fail_tick(record.run_id, "w1", "boom", now, now)

    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/diagnostics")
    assert response.status_code == 200
    assert "diag-stuck-1" in response.text
    assert "FAIL" in response.text
