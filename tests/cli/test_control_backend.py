"""``loomwork workflow ...`` against a server's control API, over ``operator``/``reader``
tokens that cannot use the worker API at all -- and the same commands with a ``worker``
token, proving the CLI's automatic backend dispatch (``loomwork.cli.backend.
open_operations``) is transparent either way.

No real socket: ``loomwork.cli.backend._test_transports`` routes the CLI's ``--server``
connection to an in-process ``httpx.ASGITransport`` for one base URL, the same technique
``tests/server/conftest.py`` uses for ``HttpStore``.
"""

from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

import loomwork as lw
from loomwork.cli import main
from loomwork.cli.backend import _test_transports
from loomwork.sdk.definitions import Registry
from loomwork.server.app import create_app
from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.factory import store_from_url
from loomwork.worker.worker import Worker, WorkerOptions

pytestmark = pytest.mark.integration

BASE_URL = "http://control-backend-test"

greet_channel = lw.Channel("greet", str)


@lw.step(name="cb.upper")
async def upper(text: str) -> str:
    return text.upper()


@lw.workflow(name="cb.greet")
async def greet_workflow(name: str) -> str:
    return await upper(name)


@lw.workflow(name="cb.sleeper")
async def sleeper() -> str:
    await lw.sleep(3600)
    return "woke"


@lw.workflow(name="cb.waits")
async def waits() -> str:
    return await greet_channel.receive()


REGISTRY = Registry(workflows=[greet_workflow, sleeper, waits], steps=[upper])


@dataclass(slots=True)
class Env:
    tokens: dict[str, str]


@pytest.fixture
async def control_env(tmp_path: Path) -> AsyncIterator[Env]:
    tokens_path = tmp_path / "tokens.json"
    file = TokenFile()
    plaintexts: dict[str, str] = {}
    for name, role in (
        ("operator", Role.OPERATOR),
        ("reader", Role.READER),
        ("worker", Role.WORKER),
        ("admin", Role.ADMIN),
    ):
        plaintext, record = generate_token(name, [role], ["default"])
        file.tokens[record.id] = record
        plaintexts[name] = plaintext
    file.save(tokens_path)

    store_url = f"sqlite:///{tmp_path}/control-backend.db"
    config = ServerConfig(
        namespaces={"default": store_url}, auth=AuthConfig(tokens_file=tokens_path)
    )
    # run_system_workers=False: each `loomwork.cli.main()` call below opens its own
    # private event loop (by design, see cli/main.py), and the ASGI app runs *in* that
    # same loop via ASGITransport (there is no real socket or thread). A background
    # system worker would poll the *same* SQLiteStore object continuously on this
    # fixture's own loop, and an `asyncio.Lock` contended across two different loops
    # raises -- not a real bug, just an artifact of driving one persistent app from many
    # short-lived loops, which a real deployment never does (one process, one loop).
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        store = store_from_url(store_url)
        await store.open()
        worker = Worker(store, REGISTRY, options=WorkerOptions(shutdown_grace=1.0))
        async with worker:
            _test_transports[BASE_URL] = httpx.ASGITransport(app=app)
            try:
                yield Env(tokens=plaintexts)
            finally:
                _test_transports.pop(BASE_URL, None)
        await store.close()


def run(
    monkeypatch: pytest.MonkeyPatch, token: str, *argv: str, stdin: str = ""
) -> tuple[int, str, str]:
    monkeypatch.setenv("LOOMWORK_SERVER_URL", BASE_URL)
    monkeypatch.setenv("LOOMWORK_TOKEN", token)
    monkeypatch.delenv("LOOMWORK_STORE_URL", raising=False)
    monkeypatch.delenv("LOOMWORK_TOKEN_FILE", raising=False)
    out, err = io.StringIO(), io.StringIO()
    code = main(list(argv), stdout=out, stderr=err, stdin=io.StringIO(stdin))
    return code, out.getvalue(), err.getvalue()


def ok(monkeypatch: pytest.MonkeyPatch, token: str, *argv: str) -> tuple[int, str, str]:
    result = run(monkeypatch, token, *argv)
    assert result[0] == 0, f"exit {result[0]}\nstdout: {result[1]}\nstderr: {result[2]}"
    return result


# --------------------------------------------------------------------------- operator


def test_operator_start_describe_list_signal_terminate(
    control_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = control_env.tokens["operator"]
    code, out, _err = ok(monkeypatch, token, "workflow", "start", "cb.waits", "--id", "op-1")
    assert "started op-1" in out

    code, out, _err = ok(monkeypatch, token, "workflow", "describe", "op-1", "--json")
    data = json.loads(out)
    assert data["workflow_id"] == "op-1"
    assert data["status"] == "running"
    assert "search_attributes" in data

    code, out, _err = ok(monkeypatch, token, "workflow", "list", "--json")
    ids = [r["workflow_id"] for r in json.loads(out)]
    assert "op-1" in ids

    code, out, err = ok(monkeypatch, token, "workflow", "signal", "op-1", "greet", '"hi"')
    assert "sent to channel" in err

    code, out, err = ok(monkeypatch, token, "workflow", "terminate", "op-1", "--reason", "done")
    assert "terminated" in err

    code, out, _err = ok(monkeypatch, token, "workflow", "describe", "op-1", "--json")
    data = json.loads(out)
    assert data["status"] == "terminated"
    assert data["close_reason"] == "done"


def test_operator_start_with_id_policies(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    token = control_env.tokens["operator"]
    ok(monkeypatch, token, "workflow", "start", "cb.sleeper", "--id", "pol-1")
    code, _out, err = run(
        monkeypatch,
        token,
        "workflow",
        "start",
        "cb.sleeper",
        "--id",
        "pol-1",
        "--id-conflict-policy",
        "use_existing",
    )
    assert code == 0

    code, _out, _err = run(
        monkeypatch,
        token,
        "workflow",
        "start",
        "cb.sleeper",
        "--id",
        "pol-1",
        "--id-conflict-policy",
        "fail",
    )
    assert code == 1  # WorkflowAlreadyStartedError


def test_operator_reset(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    token = control_env.tokens["operator"]
    ok(monkeypatch, token, "workflow", "start", "cb.waits", "--id", "reset-1")

    code, out, _err = ok(
        monkeypatch,
        token,
        "workflow",
        "reset",
        "reset-1",
        "--to",
        "first-tick",
        "--reason",
        "bug fix",
    )
    assert "reset to reset-1" in out
    new_run_id = out.split("run ")[1].rstrip(")\n")

    # "reset-1" with no --run resolves to the *latest* run of the id, which after a
    # reset is the new run (exactly like it would be after a continue-as-new or retry).
    code, out, _err = ok(monkeypatch, token, "workflow", "describe", "reset-1", "--json")
    new_data = json.loads(out)
    assert new_data["run_id"] == new_run_id
    assert new_data["reset_from_run_id"] is not None

    code, out, _err = ok(
        monkeypatch, token, "workflow", "describe", "--run", new_data["reset_from_run_id"], "--json"
    )
    old_data = json.loads(out)
    assert old_data["status"] == "terminated"
    assert old_data["continued_run_id"] == new_run_id


def test_operator_batch_cancel(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    token = control_env.tokens["operator"]
    ok(monkeypatch, token, "workflow", "start", "cb.sleeper", "--id", "batch-cb-1")
    ok(monkeypatch, token, "workflow", "start", "cb.sleeper", "--id", "batch-cb-2")

    code, out, _err = ok(
        monkeypatch,
        token,
        "workflow",
        "batch",
        "cancel",
        "-q",
        "workflow_name = 'cb.sleeper' AND status = 'running'",
        "--yes",
        "--json",
    )
    summary = json.loads(out)
    assert summary["matched"] >= 2
    assert summary["succeeded"] == summary["matched"]
    assert summary["failed"] == []


# --------------------------------------------------------------------------- reader / RBAC


def test_reader_can_list_and_describe(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    operator = control_env.tokens["operator"]
    ok(monkeypatch, operator, "workflow", "start", "cb.sleeper", "--id", "reader-1")

    reader = control_env.tokens["reader"]
    code, out, _err = ok(monkeypatch, reader, "workflow", "describe", "reader-1", "--json")
    assert json.loads(out)["workflow_id"] == "reader-1"

    code, out, _err = ok(monkeypatch, reader, "workflow", "list", "--json")
    assert any(r["workflow_id"] == "reader-1" for r in json.loads(out))


def test_reader_cannot_cancel(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    operator = control_env.tokens["operator"]
    ok(monkeypatch, operator, "workflow", "start", "cb.sleeper", "--id", "reader-cancel-1")

    reader = control_env.tokens["reader"]
    code, _out, err = run(monkeypatch, reader, "workflow", "cancel", "reader-cancel-1")
    assert code != 0
    assert "control_write" in err or "forbidden" in err.lower()


# --------------------------------------------------------------------------- worker/admin parity


def _proves_store_backed_dispatch(code: int, err: str) -> None:
    """``open_operations`` chose the store-backed path (``HttpStore``) for this token:
    the test-only ASGI transport only covers :class:`ControlClient` connections (see
    ``loomwork.cli.backend._test_transports``), so ``HttpStore`` here tries a real
    socket and fails to connect -- proof it took that branch rather than the control
    API, which would have succeeded via the in-process transport. The store-backed path
    itself (a real ``HttpStore`` against a real server) is exercised end to end by
    ``tests/server/test_http_store_conformance.py`` and friends; this test's job is only
    to prove *this* token reaches it.
    """
    from loomwork.cli.errors import ExitCode

    assert code == ExitCode.UNAVAILABLE, err
    assert "cannot reach the server" in err


def test_worker_token_is_routed_to_the_store_backed_path(
    control_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = control_env.tokens["worker"]
    code, _out, err = run(monkeypatch, token, "workflow", "start", "cb.greet", '"w"', "--id", "w-1")
    _proves_store_backed_dispatch(code, err)


def test_admin_token_is_routed_to_the_store_backed_path(
    control_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = control_env.tokens["admin"]
    code, _out, err = run(monkeypatch, token, "workflow", "start", "cb.sleeper", "--id", "adm-1")
    _proves_store_backed_dispatch(code, err)
