"""``[server.console]`` config: defaults, disabling the console, and the search-
attributes display hint."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ConsoleConfig, ServerConfig
from tests.server.console.conftest import login, running_app

pytestmark = pytest.mark.integration


def test_console_config_defaults() -> None:
    config = ConsoleConfig()
    assert config.enabled is True
    assert config.session_secret is None
    assert config.session_ttl_seconds == pytest.approx(8 * 3600)
    assert config.search_attributes == {}


def test_console_config_rejects_non_positive_ttl() -> None:
    with pytest.raises(ValueError, match="session_ttl_seconds"):
        ConsoleConfig(session_ttl_seconds=0)


async def test_console_disabled_is_not_mounted() -> None:
    config = ServerConfig(
        namespaces={"default": "memory://"},
        auth=AuthConfig(disabled=True),
        console=ConsoleConfig(enabled=False),
    )
    async with running_app(config) as (_app, client):
        response = await client.get("/console/login")
        assert response.status_code == 404
        # The rest of the server is unaffected.
        health = await client.get("/healthz")
        assert health.status_code == 200


async def test_console_enabled_by_default() -> None:
    config = ServerConfig(namespaces={"default": "memory://"}, auth=AuthConfig(disabled=True))
    assert config.console.enabled is True
    async with running_app(config) as (_app, client):
        response = await client.get("/console/login", follow_redirects=False)
        assert response.status_code == 303


async def test_search_attribute_hints_shown_on_runs_list() -> None:
    tokens_path = Path(tempfile.mkdtemp()) / "tokens.json"
    plaintext, record = generate_token("hint-op", [Role.OPERATOR], ["default"])
    TokenFile({record.id: record}).save(tokens_path)

    config = ServerConfig(
        namespaces={"default": "memory://"},
        auth=AuthConfig(tokens_file=tokens_path),
        console=ConsoleConfig(search_attributes={"customer_tier": "string", "is_trial": "bool"}),
    )
    async with running_app(config) as (_app, client):
        await login(client, plaintext)
        response = await client.get("/console/default/runs")
        assert response.status_code == 200
        assert "customer_tier (string)" in response.text
        assert "is_trial (bool)" in response.text
