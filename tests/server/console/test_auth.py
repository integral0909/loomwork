"""Login, logout, session expiry and tampered cookies."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ConsoleConfig, ServerConfig
from loomwork.server.console.session import COOKIE_NAME
from tests.server.console.conftest import ConsoleEnv, extract_csrf, login, running_app

pytestmark = pytest.mark.integration


async def test_login_page_renders(console_env: ConsoleEnv) -> None:
    response = await console_env.client.get("/console/login")
    assert response.status_code == 200
    assert "Sign in" in response.text


async def test_unauthenticated_get_redirects_to_login_with_next(console_env: ConsoleEnv) -> None:
    response = await console_env.client.get("/console/default/runs", follow_redirects=False)
    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("/console/login?next=")
    assert "%2Fconsole%2Fdefault%2Fruns" in location


async def test_login_success_sets_signed_httponly_cookie(console_env: ConsoleEnv) -> None:
    response = await login(
        console_env.client, console_env.tokens.operator, next_path="/console/default/runs"
    )
    assert response.headers["location"] == "/console/default/runs"
    set_cookie = response.headers["set-cookie"]
    assert f"{COOKIE_NAME}=" in set_cookie
    assert "httponly" in set_cookie.lower()
    assert "samesite=strict" in set_cookie.lower()
    # The test transport serves plain http://test, so Secure must not be set here.
    assert "secure" not in set_cookie.lower()


async def test_login_invalid_token_shows_error_and_escapes_it(console_env: ConsoleEnv) -> None:
    hostile = "<script>alert(1)</script>"
    response = await console_env.client.post(
        "/console/login", data={"token": hostile, "next": "/console/"}
    )
    assert response.status_code == 401
    assert "Invalid or unknown token" in response.text
    # The submitted token is never echoed back onto the page at all.
    assert hostile not in response.text
    assert COOKIE_NAME not in response.headers.get("set-cookie", "")


async def test_login_escapes_hostile_next_value(console_env: ConsoleEnv) -> None:
    hostile_next = '/console/"><script>alert(1)</script>'
    response = await console_env.client.post(
        "/console/login", data={"token": "not-a-real-token", "next": hostile_next}
    )
    assert response.status_code == 401
    assert "<script>alert(1)</script>" not in response.text
    assert "&lt;script&gt;" in response.text


async def test_login_rejects_open_redirect_next(console_env: ConsoleEnv) -> None:
    response = await login(
        console_env.client, console_env.tokens.operator, next_path="https://evil.example/steal"
    )
    assert response.headers["location"] == "/console/"


async def test_logout_clears_cookie_and_requires_new_login(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/runs")
    csrf = extract_csrf(page.text)
    response = await console_env.client.post(
        "/console/logout", data={"csrf_token": csrf}, follow_redirects=False
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/console/login")
    assert "max-age=0" in response.headers["set-cookie"].lower()

    again = await console_env.client.get("/console/default/runs", follow_redirects=False)
    assert again.status_code == 303
    assert again.headers["location"].startswith("/console/login")


async def test_logout_rejects_bad_csrf(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.post("/console/logout", data={"csrf_token": "wrong"})
    assert response.status_code == 403


async def test_home_redirects_to_first_accessible_namespace(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/console/billing/runs"


async def test_token_with_no_accessible_namespace_gets_403() -> None:
    tokens_path = Path(tempfile.mkdtemp()) / "tokens.json"
    plaintext, record = generate_token("nowhere-1", [Role.READER], ["ghost-namespace"])
    TokenFile({record.id: record}).save(tokens_path)
    config = ServerConfig(
        namespaces={"default": "memory://"}, auth=AuthConfig(tokens_file=tokens_path)
    )
    async with running_app(config) as (_app, client):
        await login(client, plaintext)
        response = await client.get("/console/")
        assert response.status_code == 403
        assert "no accessible namespaces" in response.text.lower()


async def test_tampered_session_cookie_is_rejected(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    good = console_env.client.cookies[COOKIE_NAME]
    console_env.client.cookies.set(COOKIE_NAME, good[:-2] + "xx")
    response = await console_env.client.get("/console/default/runs", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"].startswith("/console/login")


async def test_malformed_session_cookie_is_rejected(console_env: ConsoleEnv) -> None:
    console_env.client.cookies.set(COOKIE_NAME, "not-even-signed")
    response = await console_env.client.get("/console/default/runs", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"].startswith("/console/login")


async def test_auth_disabled_skips_login_and_shows_banner() -> None:
    config = ServerConfig(namespaces={"default": "memory://"}, auth=AuthConfig(disabled=True))
    async with running_app(config) as (_app, client):
        login_page = await client.get("/console/login", follow_redirects=False)
        assert login_page.status_code == 303
        assert login_page.headers["location"] == "/console/"

        response = await client.get("/console/default/runs")
        assert response.status_code == 200
        assert "auth.disabled" in response.text
        assert "Sign out" not in response.text


async def test_session_ttl_enforced_with_real_login() -> None:
    tokens_path = Path(tempfile.mkdtemp()) / "tokens.json"
    plaintext, record = generate_token("short-1", [Role.OPERATOR], ["default"])
    TokenFile({record.id: record}).save(tokens_path)
    config = ServerConfig(
        namespaces={"default": "memory://"},
        auth=AuthConfig(tokens_file=tokens_path),
        console=ConsoleConfig(session_ttl_seconds=0.2),
    )
    async with running_app(config) as (_app, client):
        await login(client, plaintext, next_path="/console/default/runs")
        fresh = await client.get("/console/default/runs", follow_redirects=False)
        assert fresh.status_code == 200

        await asyncio.sleep(0.35)
        stale = await client.get("/console/default/runs", follow_redirects=False)
        assert stale.status_code == 303
        assert stale.headers["location"].startswith("/console/login")
