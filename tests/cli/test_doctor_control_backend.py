"""``loomwork doctor --server``, over the control API (see ``tests/cli/
test_control_backend.py`` for the general in-process-transport approach this mirrors)."""

from __future__ import annotations

import asyncio
import io
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from loomwork.cli import main
from loomwork.cli.backend import _test_transports
from loomwork.engine.engine import Engine
from loomwork.server.app import create_app
from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.factory import store_from_url

pytestmark = pytest.mark.integration

BASE_URL = "http://doctor-control-backend-test"


@dataclass(slots=True)
class Env:
    tokens: dict[str, str]
    store_url: str


@pytest.fixture
async def doctor_env(tmp_path: Path) -> AsyncIterator[Env]:
    tokens_path = tmp_path / "tokens.json"
    file = TokenFile()
    plaintexts: dict[str, str] = {}
    for name, role in (
        ("operator", Role.OPERATOR),
        ("reader", Role.READER),
        ("worker", Role.WORKER),
    ):
        plaintext, record = generate_token(name, [role], ["default"])
        file.tokens[record.id] = record
        plaintexts[name] = plaintext
    file.save(tokens_path)

    store_url = f"sqlite:///{tmp_path}/doctor-control-backend.db"
    config = ServerConfig(
        namespaces={"default": store_url}, auth=AuthConfig(tokens_file=tokens_path)
    )
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        _test_transports[BASE_URL] = httpx.ASGITransport(app=app)
        try:
            yield Env(tokens=plaintexts, store_url=store_url)
        finally:
            _test_transports.pop(BASE_URL, None)


def run(monkeypatch: pytest.MonkeyPatch, token: str, *argv: str) -> tuple[int, str, str]:
    monkeypatch.setenv("LOOMWORK_SERVER_URL", BASE_URL)
    monkeypatch.setenv("LOOMWORK_TOKEN", token)
    monkeypatch.delenv("LOOMWORK_STORE_URL", raising=False)
    monkeypatch.delenv("LOOMWORK_TOKEN_FILE", raising=False)
    out, err = io.StringIO(), io.StringIO()
    code = main(list(argv), stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def test_operator_doctor_reports_healthy(doctor_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    code, out, err = run(monkeypatch, doctor_env.tokens["operator"], "doctor", "--json")
    assert code == 0, err
    data = json.loads(out)
    assert data["ok"] is True
    checks = {f["check"] for f in data["findings"]}
    assert "token" in checks
    assert "clock skew" in checks
    token_finding = next(f for f in data["findings"] if f["check"] == "token")
    assert "operator" in token_finding["message"]
    assert "default" in token_finding["message"]


def test_reader_doctor_reports_healthy(doctor_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    code, out, err = run(monkeypatch, doctor_env.tokens["reader"], "doctor")
    assert code == 0, err
    assert "FAIL" not in out


def test_worker_token_cannot_run_doctor(doctor_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    code, _out, err = run(monkeypatch, doctor_env.tokens["worker"], "doctor")
    assert code != 0
    assert "control_read" in err or "forbidden" in err.lower()


def test_doctor_over_server_finds_a_real_tick_failure(
    doctor_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def setup() -> None:
        store = store_from_url(doctor_env.store_url)
        await store.open()
        try:
            engine = Engine(store)
            record = await engine.start_workflow("doctor.stuck", [], workflow_id="server-stuck-1")
            now = datetime.now(UTC)
            claimed = await store.claim_tick("w1", now, timedelta(seconds=30))
            assert claimed is not None and claimed.run_id == record.run_id
            await store.fail_tick(record.run_id, "w1", "boom", now, now)
        finally:
            await store.close()

    with asyncio.Runner(loop_factory=asyncio.new_event_loop) as runner:
        runner.run(setup())

    code, out, err = run(
        monkeypatch,
        doctor_env.tokens["operator"],
        "doctor",
        "--tick-failure-threshold",
        "1",
        "--json",
    )
    assert code == 1, err
    data = json.loads(out)
    assert data["ok"] is False
    tick_finding = next(f for f in data["findings"] if f["check"] == "tick failures")
    assert tick_finding["status"] == "fail"
    assert "server-stuck-1" in tick_finding["message"]
