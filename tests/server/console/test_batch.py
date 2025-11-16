"""The batch page: previewing a visibility query, then cancelling or terminating every
match, with the same CSRF/RBAC/escaping guarantees as the single-run actions."""

from __future__ import annotations

import pytest

from tests.server.console.conftest import ConsoleEnv, extract_csrf, login, start_echo

pytestmark = pytest.mark.integration


async def _login_operator(env: ConsoleEnv) -> None:
    await login(env.client, env.tokens.operator)


async def test_batch_page_without_query_shows_the_form_only(console_env: ConsoleEnv) -> None:
    await _login_operator(console_env)
    response = await console_env.client.get("/console/default/batch")
    assert response.status_code == 200
    assert "Visibility query" in response.text
    assert "Matches" not in response.text


async def test_batch_preview_lists_matching_runs(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "batch-preview-1")
    await _login_operator(console_env)
    response = await console_env.client.get(
        "/console/default/batch", params={"q": "status = 'running'"}
    )
    assert response.status_code == 200
    assert record.workflow_id in response.text
    assert "Cancel every match" in response.text
    assert "Terminate every match" in response.text


async def test_batch_preview_invalid_query_shows_error_not_500(console_env: ConsoleEnv) -> None:
    await _login_operator(console_env)
    response = await console_env.client.get(
        "/console/default/batch", params={"q": "status = = 'x'"}
    )
    assert response.status_code == 200
    assert "banner-error" in response.text
    assert "No runs match." in response.text


async def test_batch_cancel_applies_to_every_match(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    a = await start_echo(ns.engine, "batch-cancel-a")
    b = await start_echo(ns.engine, "batch-cancel-b")
    await _login_operator(console_env)
    query = "workflow_name = 'console.echo' AND status = 'running'"
    preview = await console_env.client.get("/console/default/batch", params={"q": query})
    csrf = extract_csrf(preview.text)

    response = await console_env.client.post(
        "/console/default/batch/cancel",
        data={"csrf_token": csrf, "query": query, "reason": "fleet cleanup"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "level=success" in response.headers["location"]

    for record in (a, b):
        updated = await ns.engine.describe(record.run_id)
        assert updated.cancel_requested is True


async def test_batch_terminate_requires_confirmation(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "batch-term-noconfirm-1")
    await _login_operator(console_env)
    query = "status = 'running'"
    preview = await console_env.client.get("/console/default/batch", params={"q": query})
    csrf = extract_csrf(preview.text)

    response = await console_env.client.post(
        "/console/default/batch/terminate",
        data={"csrf_token": csrf, "query": query, "reason": "bye"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "level=error" in response.headers["location"]
    still_running = await ns.engine.describe(record.run_id)
    assert still_running.status.value == "running"


async def test_batch_terminate_with_confirmation_closes_matches(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "batch-term-confirm-1")
    await _login_operator(console_env)
    query = f"workflow_id = '{record.workflow_id}'"
    preview = await console_env.client.get("/console/default/batch", params={"q": query})
    csrf = extract_csrf(preview.text)

    response = await console_env.client.post(
        "/console/default/batch/terminate",
        data={"csrf_token": csrf, "query": query, "reason": "bye", "confirm": "yes"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "level=success" in response.headers["location"]
    terminated = await ns.engine.describe(record.run_id)
    assert terminated.status.value == "terminated"


async def test_reader_cannot_act_but_can_preview(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "batch-rbac-reader-1")
    await login(console_env.client, console_env.tokens.reader)

    preview = await console_env.client.get(
        "/console/default/batch", params={"q": "status = 'running'"}
    )
    assert preview.status_code == 200
    assert record.workflow_id in preview.text
    assert "Cancel every match" not in preview.text

    response = await console_env.client.post(
        "/console/default/batch/cancel",
        data={"csrf_token": "irrelevant", "query": "status = 'running'", "reason": "nope"},
    )
    assert response.status_code == 403
    still = await ns.engine.describe(record.run_id)
    assert still.cancel_requested is False


async def test_batch_csrf_wrong_token_rejected(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "batch-csrf-1")
    await _login_operator(console_env)

    response = await console_env.client.post(
        "/console/default/batch/cancel",
        data={"csrf_token": "wrong", "query": "status = 'running'", "reason": "x"},
    )
    assert response.status_code == 403
    still = await ns.engine.describe(record.run_id)
    assert still.cancel_requested is False


async def test_batch_preview_escapes_hostile_workflow_id(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    hostile_id = "<script>alert('batch')</script>"
    await start_echo(ns.engine, hostile_id)
    await _login_operator(console_env)
    response = await console_env.client.get(
        "/console/default/batch", params={"q": "status = 'running'"}
    )
    assert response.status_code == 200
    assert "<script>alert" not in response.text
    assert "&lt;script&gt;" in response.text
