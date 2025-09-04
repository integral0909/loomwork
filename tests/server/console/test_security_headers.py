"""Every console response -- pages, redirects, error pages, and the static stylesheet --
carries the same security headers."""

from __future__ import annotations

import pytest

from tests.server.console.conftest import ConsoleEnv, login, start_echo

pytestmark = pytest.mark.integration

EXPECTED_HEADERS = {
    "content-security-policy": "default-src 'none'",
    "x-frame-options": "DENY",
    "referrer-policy": "no-referrer",
    "x-content-type-options": "nosniff",
}


def _assert_security_headers(headers: object) -> None:
    for name, expected_prefix in EXPECTED_HEADERS.items():
        value = headers[name]  # type: ignore[index]
        assert value.startswith(expected_prefix) or value == expected_prefix, (name, value)


async def test_login_page_has_security_headers(console_env: ConsoleEnv) -> None:
    response = await console_env.client.get("/console/login")
    _assert_security_headers(response.headers)


async def test_runs_list_has_security_headers(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/runs")
    _assert_security_headers(response.headers)


async def test_run_detail_has_security_headers(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "headers-1")
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    _assert_security_headers(response.headers)


async def test_404_error_page_has_security_headers(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/runs/does-not-exist")
    assert response.status_code == 404
    _assert_security_headers(response.headers)


async def test_redirect_has_security_headers(console_env: ConsoleEnv) -> None:
    response = await console_env.client.get("/console/default/runs", follow_redirects=False)
    assert response.status_code == 303
    _assert_security_headers(response.headers)


async def test_static_css_has_security_headers_and_correct_type(console_env: ConsoleEnv) -> None:
    response = await console_env.client.get("/console/static/console.css")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/css")
    _assert_security_headers(response.headers)


async def test_csp_forbids_inline_scripts_and_framing(console_env: ConsoleEnv) -> None:
    response = await console_env.client.get("/console/login")
    csp = response.headers["content-security-policy"]
    assert "script-src" not in csp  # default-src 'none' already forbids every script
    assert "frame-ancestors 'none'" in csp
    assert "<script" not in response.text
