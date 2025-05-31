"""Token hashing, RBAC and the auth-disabled escape hatch, both unit-level and over HTTP."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from starlette.applications import Starlette

from loomwork.server.app import create_app
from loomwork.server.auth import (
    ForbiddenError,
    InvalidTokenError,
    Role,
    TokenFile,
    TokenRecord,
    generate_token,
    require,
    verify_token,
)
from loomwork.server.config import AuthConfig, ServerConfig

# --------------------------------------------------------------------------- unit level


def test_generate_token_shape() -> None:
    plaintext, record = generate_token("op-1", [Role.OPERATOR], ["default"])
    assert plaintext.startswith("lwk_op-1_")
    assert record.id == "op-1"
    assert record.roles == (Role.OPERATOR,)
    assert record.namespaces == ("default",)
    # The plaintext secret never appears in the stored record.
    secret = plaintext.rsplit("_", 1)[-1]
    assert secret not in record.hash
    assert secret not in record.salt


def test_verify_token_accepts_correct_and_rejects_wrong_secret() -> None:
    plaintext, record = generate_token("op-1", [Role.OPERATOR], ["default"])
    assert verify_token(plaintext, record)
    assert not verify_token("lwk_op-1_wrongsecret", record)
    assert not verify_token("lwk_other-id_" + plaintext.rsplit("_", 1)[-1], record)
    assert not verify_token("not-even-a-token", record)


def test_generate_token_validates_arguments() -> None:
    with pytest.raises(ValueError, match="invalid token id"):
        generate_token("bad id!", [Role.OPERATOR], ["default"])
    with pytest.raises(ValueError, match="at least one role"):
        generate_token("op-1", [], ["default"])
    with pytest.raises(ValueError, match="at least one namespace"):
        generate_token("op-1", [Role.OPERATOR], [])


def test_token_record_namespace_and_capability_checks() -> None:
    _, record = generate_token("op-1", [Role.OPERATOR], ["default", "billing"])
    assert record.allows_namespace("default")
    assert record.allows_namespace("billing")
    assert not record.allows_namespace("other")
    assert record.grants("control_read")
    assert record.grants("control_write")
    assert not record.grants("worker")

    _, wildcard = generate_token("admin-1", [Role.ADMIN], ["*"])
    assert wildcard.allows_namespace("anything")
    assert wildcard.grants("worker")
    assert wildcard.grants("control_write")


def test_require_raises_on_wrong_namespace_or_capability() -> None:
    _, record = generate_token("worker-1", [Role.WORKER], ["default"])
    require(record, capability="worker", namespace="default")  # does not raise
    with pytest.raises(ForbiddenError, match="namespace"):
        require(record, capability="worker", namespace="other")
    with pytest.raises(ForbiddenError, match="does not grant"):
        require(record, capability="control_write", namespace="default")


def test_token_file_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    _, record = generate_token("op-1", [Role.OPERATOR], ["default"])
    file = TokenFile({record.id: record})
    file.save(path)

    loaded = TokenFile.load(path)
    assert loaded.tokens[record.id] == record
    # Stored as plain, readable JSON (no secrets, since only the hash is ever written).
    raw = json.loads(path.read_text())
    assert raw[0]["id"] == "op-1"
    assert "hash" in raw[0]
    assert "salt" in raw[0]


def test_token_file_authenticate(tmp_path: Path) -> None:
    plaintext, record = generate_token("op-1", [Role.OPERATOR], ["default"])
    file = TokenFile({record.id: record})
    authenticated = file.authenticate(plaintext)
    assert authenticated == record
    with pytest.raises(InvalidTokenError):
        file.authenticate("lwk_op-1_wrong")
    with pytest.raises(InvalidTokenError):
        file.authenticate("lwk_unknown-id_secret")
    with pytest.raises(InvalidTokenError):
        file.authenticate("garbage")


def test_token_record_serialization_round_trip() -> None:
    _, record = generate_token("op-1", [Role.OPERATOR, Role.READER], ["default"], description="d")
    restored = TokenRecord.from_dict(record.to_dict())
    assert restored == record


# --------------------------------------------------------------------------- over HTTP


def _server(tmp_path: Path, *, disabled: bool = False) -> tuple[Starlette, str, str]:
    tokens_path = tmp_path / "tokens.json"
    worker_token, worker_record = generate_token("worker-1", [Role.WORKER], ["default"])
    reader_token, reader_record = generate_token("reader-1", [Role.READER], ["default"])
    TokenFile({worker_record.id: worker_record, reader_record.id: reader_record}).save(tokens_path)
    auth = AuthConfig(disabled=disabled) if disabled else AuthConfig(tokens_file=tokens_path)
    config = ServerConfig(namespaces={"default": "memory://"}, auth=auth)
    app = create_app(config, run_system_workers=False)
    return app, worker_token, reader_token


async def test_missing_token_is_401(tmp_path: Path) -> None:
    app, _worker_token, _reader_token = _server(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        response = await http.get("/v1/namespaces/default/executions/r1")
        assert response.status_code == 401
        assert response.json()["code"] == "unauthorized"


async def test_invalid_token_is_401(tmp_path: Path) -> None:
    app, _worker_token, _reader_token = _server(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        response = await http.get(
            "/v1/namespaces/default/executions/r1",
            headers={"Authorization": "Bearer lwk_nope_secret"},
        )
        assert response.status_code == 401


async def test_malformed_authorization_header_is_401(tmp_path: Path) -> None:
    app, _worker_token, _reader_token = _server(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        response = await http.get(
            "/v1/namespaces/default/executions/r1", headers={"Authorization": "Token abc"}
        )
        assert response.status_code == 401


async def test_wrong_role_is_403(tmp_path: Path) -> None:
    app, _worker_token, reader_token = _server(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        # A reader token may not use the worker API.
        response = await http.get(
            "/v1/namespaces/default/executions/r1",
            headers={"Authorization": f"Bearer {reader_token}"},
        )
        assert response.status_code == 403
        assert response.json()["code"] == "forbidden"


async def test_wrong_namespace_is_403(tmp_path: Path) -> None:
    app, worker_token, _reader_token = _server(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        response = await http.get(
            "/v1/namespaces/other/executions/r1",
            headers={"Authorization": f"Bearer {worker_token}"},
        )
        # The namespace itself does not exist either, but authorization is checked
        # first: a token scoped to "default" must not learn whether "other" exists.
        assert response.status_code == 403


async def test_reader_can_read_control_api(tmp_path: Path) -> None:
    app, _worker_token, reader_token = _server(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        response = await http.get(
            "/v1/namespaces/default/control/workflows",
            headers={"Authorization": f"Bearer {reader_token}"},
        )
        assert response.status_code == 200
        # But not write to it.
        response = await http.post(
            "/v1/namespaces/default/control/workflows",
            json={"name": "wf", "args": []},
            headers={"Authorization": f"Bearer {reader_token}"},
        )
        assert response.status_code == 403


async def test_auth_disabled_grants_everything(tmp_path: Path) -> None:
    app, _worker_token, _reader_token = _server(tmp_path, disabled=True)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        response = await http.get("/v1/namespaces/default/executions/r1")
        # No token at all, yet the request reaches the store (and 404s there, not
        # on authorization).
        assert response.status_code == 404
        assert response.json()["code"] == "not_found"


def test_config_requires_tokens_file_unless_disabled() -> None:
    with pytest.raises(ValueError, match="tokens_file"):
        ServerConfig(namespaces={"default": "memory://"}, auth=AuthConfig())


def test_ids_with_underscores_are_rejected_because_underscore_separates_parts() -> None:
    import pytest

    from loomwork.server.auth import generate_token

    with pytest.raises(ValueError, match="letters, digits and -"):
        generate_token("my_worker", ["worker"], ["*"])


def test_hyphenated_ids_round_trip() -> None:
    from loomwork.server.auth import TokenFile, generate_token

    plaintext, record = generate_token("eu-west-worker-7", ["worker"], ["*"])
    tokens = TokenFile({record.id: record})
    assert tokens.authenticate(plaintext).id == "eu-west-worker-7"


def test_verification_is_fast() -> None:
    import time

    from loomwork.server.auth import TokenFile, generate_token

    plaintext, record = generate_token("fast", ["worker"], ["*"])
    tokens = TokenFile({record.id: record})
    start = time.perf_counter()
    for _ in range(1000):
        tokens.authenticate(plaintext)
    # Workers authenticate on every call; a thousand checks must be cheap.
    assert time.perf_counter() - start < 0.5
