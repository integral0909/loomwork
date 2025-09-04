"""Role-based access control and CSRF protection, exercised over HTTP.

Mirrors the RBAC checks in ``tests/server/test_e2e_worker.py`` (worker token cannot use
the control API, and vice versa) for the console: a reader can view but not act, and a
token scoped to a different namespace is forbidden from one it does not list, matching
exactly what the control and worker APIs already do (see ``loomwork.server.auth.require``,
reused directly by every console view).
"""

from __future__ import annotations

import pytest

from tests.server.console.conftest import ConsoleEnv, extract_csrf, login, start_echo

pytestmark = pytest.mark.integration


async def test_reader_can_view_runs_list(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.get("/console/default/runs")
    assert response.status_code == 200


async def test_reader_cannot_cancel_a_run(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "rbac-reader-1")

    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/cancel",
        data={"csrf_token": "irrelevant", "reason": "nope"},
    )
    assert response.status_code == 403


async def test_reader_cannot_see_signal_form(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "rbac-reader-2")

    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert response.status_code == 200
    assert "Send signal" not in response.text
    assert "Terminate" not in response.text


async def test_token_scoped_to_other_namespace_is_forbidden(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.other_namespace_operator)
    response = await console_env.client.get("/console/default/runs")
    assert response.status_code == 403


async def test_worker_role_cannot_view_console(console_env: ConsoleEnv) -> None:
    # Mirrors test_e2e_worker.test_worker_token_cannot_use_control_api: the worker role
    # grants HttpStore's API, not the operator control surface the console itself uses.
    await login(console_env.client, console_env.tokens.worker)
    response = await console_env.client.get("/console/default/runs")
    assert response.status_code == 403


async def test_operator_can_view_and_act(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "rbac-operator-1")

    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert page.status_code == 200
    assert "Send signal" in page.text
    csrf = extract_csrf(page.text)
    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/cancel",
        data={"csrf_token": csrf, "reason": "cleanup"},
        follow_redirects=False,
    )
    assert response.status_code == 303


async def test_csrf_missing_token_rejected(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "csrf-missing-1")
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/cancel", data={"reason": "x"}
    )
    assert response.status_code == 403


async def test_csrf_wrong_token_rejected(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "csrf-wrong-1")
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/cancel",
        data={"csrf_token": "totally-wrong", "reason": "x"},
    )
    assert response.status_code == 403
    still = await ns.engine.describe(record.run_id)
    assert still.cancel_requested is False


async def test_csrf_correct_token_accepted(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "csrf-correct-1")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)
    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/cancel",
        data={"csrf_token": csrf, "reason": "x"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    updated = await ns.engine.describe(record.run_id)
    assert updated.cancel_requested is True
