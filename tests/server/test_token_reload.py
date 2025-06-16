"""Revoking or adding a token takes effect on a running server without a restart."""

from __future__ import annotations

import os
import time
from pathlib import Path

import httpx
import pytest

from loomwork.server.auth import TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.server.state import ServerState
from tests.server.conftest import make_tokens_file, running_app


def config_for(tokens: Path) -> ServerConfig:
    return ServerConfig(namespaces={"default": "memory://"}, auth=AuthConfig(tokens_file=tokens))


def bump_mtime(path: Path) -> None:
    # Coarse filesystem timestamps could hide a rewrite within the same tick.
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))


@pytest.fixture(autouse=True)
def _no_recheck_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ServerState, "TOKENS_RECHECK_INTERVAL", 0.0)


async def status(app: object, token: str) -> int:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/v1/namespaces/default/executions", headers={"Authorization": f"Bearer {token}"}
        )
    return response.status_code


async def test_revoked_token_is_rejected_without_restart(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    token = make_tokens_file(path, ("w1", ["worker"], ["default"]))
    async with running_app(config_for(path)) as app:
        assert await status(app, token) == 200

        TokenFile().save(path)
        bump_mtime(path)
        assert await status(app, token) == 401


async def test_new_token_is_accepted_without_restart(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    make_tokens_file(path, ("w1", ["worker"], ["default"]))
    async with running_app(config_for(path)) as app:
        plaintext, record = generate_token("w2", ["worker"], ["default"])
        assert await status(app, plaintext) == 401

        tokens = TokenFile.load(path)
        tokens.tokens[record.id] = record
        tokens.save(path)
        bump_mtime(path)
        assert await status(app, plaintext) == 200


async def test_unparsable_file_keeps_previous_tokens(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "tokens.json"
    token = make_tokens_file(path, ("w1", ["worker"], ["default"]))
    async with running_app(config_for(path)) as app:
        path.write_text("[{half written")
        bump_mtime(path)
        assert await status(app, token) == 200
        assert "ignoring unreadable tokens file" in caplog.text


async def test_deleted_file_keeps_previous_tokens(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    token = make_tokens_file(path, ("w1", ["worker"], ["default"]))
    async with running_app(config_for(path)) as app:
        path.unlink()
        assert await status(app, token) == 200


def test_checks_are_rate_limited(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "tokens.json"
    make_tokens_file(path, ("w1", ["worker"], ["default"]))
    state = ServerState(config_for(path), run_system_workers=False)
    monkeypatch.setattr(ServerState, "TOKENS_RECHECK_INTERVAL", 3600.0)
    state._tokens_checked = 0.0
    first = state.current_tokens()
    TokenFile().save(path)
    bump_mtime(path)
    # Within the interval, the file is not even looked at.
    state._tokens_checked = time.monotonic()
    assert state.current_tokens() is first
