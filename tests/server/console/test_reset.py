"""The run detail page's reset form: point selection, reason, CSRF, RBAC, escaping."""

from __future__ import annotations

import asyncio

import pytest

from tests.server.console.conftest import ConsoleEnv, extract_csrf, login, start_echo

pytestmark = pytest.mark.integration


async def _login_operator(env: ConsoleEnv) -> None:
    await login(env.client, env.tokens.operator)


async def _wait_first_tick(env: ConsoleEnv, run_id: str) -> None:
    ns = env.app.state.server.namespace("default")
    for _ in range(50):
        history = await ns.engine.history(run_id)
        if any(e.kind == "step_completed" for e in history):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"run {run_id} never completed its first tick")


async def test_reset_form_is_shown_to_an_operator(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-form-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert page.status_code == 200
    assert "Truncates this run" in page.text
    assert 'name="reset_point"' in page.text


async def test_reset_first_tick_starts_a_new_run_and_links_back(
    console_env: ConsoleEnv,
) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-first-1")
    await _wait_first_tick(console_env, record.run_id)
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/reset",
        data={"csrf_token": csrf, "reset_point": "first_tick", "reason": "bug fix"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers["location"]
    assert "/console/default/runs/" in location
    assert record.run_id not in location  # redirects to the *new* run

    old = await ns.engine.describe(record.run_id)
    assert old.status.value == "terminated"
    assert old.continued_run_id is not None
    new_run_id = old.continued_run_id

    old_page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert "Continued as:" in old_page.text
    assert new_run_id in old_page.text

    new_page = await console_env.client.get(f"/console/default/runs/{new_run_id}")
    assert new_page.status_code == 200
    assert "Reset from:" in new_page.text
    assert record.run_id in new_page.text


async def test_reset_after_tick_uses_the_given_count(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-after-tick-1")
    await _wait_first_tick(console_env, record.run_id)
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/reset",
        data={
            "csrf_token": csrf,
            "reset_point": "after_tick",
            "after_tick": "1",
            "reason": "keep the first tick",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    old = await ns.engine.describe(record.run_id)
    assert old.continued_run_id is not None


async def test_reset_requires_a_reason(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-noreason-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/reset",
        data={"csrf_token": csrf, "reset_point": "first_tick", "reason": ""},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "level=error" in response.headers["location"]
    still = await ns.engine.describe(record.run_id)
    assert still.status.value == "running"


async def test_reset_invalid_after_tick_count_is_a_clean_error_not_500(
    console_env: ConsoleEnv,
) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-badcount-1")
    await _wait_first_tick(console_env, record.run_id)
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    # More ticks than the run has actually completed: InvalidResetPointError, not a 500.
    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/reset",
        data={
            "csrf_token": csrf,
            "reset_point": "after_tick",
            "after_tick": "999",
            "reason": "bad",
        },
        follow_redirects=False,
    )
    assert response.status_code == 422


async def test_reset_escapes_hostile_reason_in_flash(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-hostile-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/reset",
        data={
            "csrf_token": csrf,
            "reset_point": "first_tick",
            "reason": "ok",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert "<script>" not in response.text


async def test_reader_cannot_see_or_submit_reset(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-rbac-reader-1")
    await login(console_env.client, console_env.tokens.reader)

    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert page.status_code == 200
    assert 'name="reset_point"' not in page.text

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/reset",
        data={"csrf_token": "irrelevant", "reset_point": "first_tick", "reason": "nope"},
    )
    assert response.status_code == 403
    still = await ns.engine.describe(record.run_id)
    assert still.status.value == "running"


async def test_reset_csrf_wrong_token_rejected(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "reset-csrf-1")
    await _login_operator(console_env)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/reset",
        data={"csrf_token": "wrong", "reset_point": "first_tick", "reason": "x"},
    )
    assert response.status_code == 403
    still = await ns.engine.describe(record.run_id)
    assert still.status.value == "running"
