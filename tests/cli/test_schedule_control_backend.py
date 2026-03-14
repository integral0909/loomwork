"""``loomwork schedule ...`` against a server's control API, over ``operator``/``reader``
tokens -- the schedule equivalent of ``tests/cli/test_control_backend.py`` (see its module
docstring for the general approach: an in-process ASGI transport via
``loomwork.cli.backend._test_transports``, no real socket).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

import loomwork as lw
from loomwork.cli import main
from loomwork.cli.backend import _test_transports
from loomwork.cli.errors import ExitCode
from loomwork.sdk.definitions import Registry
from loomwork.server.app import create_app
from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.store.factory import store_from_url

pytestmark = pytest.mark.integration

BASE_URL = "http://schedule-control-backend-test"


@lw.workflow(name="sb.greet")
async def greet_workflow(name: str) -> str:
    return name.upper()


REGISTRY = Registry(workflows=[greet_workflow])


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
    ):
        plaintext, record = generate_token(name, [role], ["default"])
        file.tokens[record.id] = record
        plaintexts[name] = plaintext
    file.save(tokens_path)

    store_url = f"sqlite:///{tmp_path}/schedule-control-backend.db"
    config = ServerConfig(
        namespaces={"default": store_url}, auth=AuthConfig(tokens_file=tokens_path)
    )
    # No system worker: nothing races the schedules these tests create and inspect, and
    # a schedule command never needs one anyway (it only reads/writes the schedule
    # document itself, never runs the workflow it names).
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        store = store_from_url(store_url)
        await store.open()
        _test_transports[BASE_URL] = httpx.ASGITransport(app=app)
        try:
            yield Env(tokens=plaintexts)
        finally:
            _test_transports.pop(BASE_URL, None)
        await store.close()


def run(
    monkeypatch: pytest.MonkeyPatch, token: str, *argv: str, stdin: str = ""
) -> tuple[int, str, str]:
    import io

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


def test_operator_create_list_describe(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    token = control_env.tokens["operator"]
    _code, out, _err = ok(
        monkeypatch,
        token,
        "schedule",
        "create",
        "nightly",
        "sb.greet",
        '"a"',
        "--cron",
        "0 3 * * *",
        "--tz",
        "Europe/Stockholm",
        "--note",
        "reports",
        "--json",
    )
    created = json.loads(out)
    assert created["schedule_id"] == "nightly"
    assert created["next_fire_at"]
    assert created["policy"]["overlap"] == "skip"

    _code, out, _err = ok(monkeypatch, token, "schedule", "list", "--json")
    ids = [s["schedule_id"] for s in json.loads(out)]
    assert ids == ["nightly"]

    _code, out, _err = ok(monkeypatch, token, "schedule", "describe", "nightly", "--json")
    described = json.loads(out)
    assert described["action"]["workflow_name"] == "sb.greet"
    assert described["note"] == "reports"
    assert described["spec"]["cron"] == "0 3 * * *"
    assert described["spec"]["timezone"] == "Europe/Stockholm"

    # Plain-text mode decodes the action's arguments for display (JSON mode dumps the
    # schedule document as-is, wire-form payloads included -- true of the store-backed
    # path too, see tests/cli/test_schedule_commands.py::test_create_list_and_describe).
    _code, out, _err = ok(monkeypatch, token, "schedule", "describe", "nightly")
    assert '["a"]' in out


def test_operator_pause_unpause_trigger_backfill_upcoming_update_delete(
    control_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = control_env.tokens["operator"]
    ok(
        monkeypatch,
        token,
        "schedule",
        "create",
        "hourly",
        "sb.greet",
        '"a"',
        "--every",
        "1h",
    )

    _code, out, _err = ok(
        monkeypatch, token, "schedule", "pause", "hourly", "--note", "maintenance", "--json"
    )
    paused = json.loads(out)
    assert paused["paused"] is True
    assert paused["note"] == "maintenance"

    _code, out, _err = ok(monkeypatch, token, "schedule", "unpause", "hourly", "--json")
    assert json.loads(out)["paused"] is False

    _code, out, _err = ok(monkeypatch, token, "schedule", "trigger", "hourly", "--json")
    triggered = json.loads(out)
    assert triggered["workflow_id"]
    assert triggered["run_id"]

    _code, out, _err = ok(monkeypatch, token, "schedule", "upcoming", "hourly", "-n", "2", "--json")
    upcoming = json.loads(out)
    assert len(upcoming) == 2

    _code, out, _err = ok(
        monkeypatch,
        token,
        "schedule",
        "backfill",
        "hourly",
        "--from",
        "2024-01-01T00:00:00Z",
        "--to",
        "2024-01-01T03:00:00Z",
        "--json",
    )
    backfilled = json.loads(out)
    assert backfilled["started"] == 3

    _code, out, _err = ok(
        monkeypatch, token, "schedule", "update", "hourly", "--every", "30m", "--json"
    )
    updated = json.loads(out)
    assert updated["spec"]["interval"] == 1800.0

    code, _out, err = run(monkeypatch, token, "schedule", "delete", "hourly")
    assert code == 0
    assert "deleted schedule hourly" in err

    code, _out, err = run(monkeypatch, token, "schedule", "describe", "hourly")
    assert code == ExitCode.NOT_FOUND


# --------------------------------------------------------------------------- reader / RBAC


def test_reader_can_list_and_describe(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    operator = control_env.tokens["operator"]
    ok(monkeypatch, operator, "schedule", "create", "reader-1", "sb.greet", '"a"', "--every", "1h")

    reader = control_env.tokens["reader"]
    _code, out, _err = ok(monkeypatch, reader, "schedule", "list", "--json")
    assert [s["schedule_id"] for s in json.loads(out)] == ["reader-1"]

    _code, out, _err = ok(monkeypatch, reader, "schedule", "describe", "reader-1", "--json")
    assert json.loads(out)["schedule_id"] == "reader-1"

    _code, out, _err = ok(monkeypatch, reader, "schedule", "upcoming", "reader-1", "--json")
    assert json.loads(out)


def test_reader_cannot_pause_or_create(control_env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    operator = control_env.tokens["operator"]
    ok(monkeypatch, operator, "schedule", "create", "reader-2", "sb.greet", '"a"', "--every", "1h")

    reader = control_env.tokens["reader"]
    code, _out, err = run(monkeypatch, reader, "schedule", "pause", "reader-2")
    assert code != 0
    assert "control_write" in err or "forbidden" in err.lower()

    code, _out, err = run(
        monkeypatch, reader, "schedule", "create", "reader-3", "sb.greet", '"a"', "--every", "1h"
    )
    assert code != 0
    assert "control_write" in err or "forbidden" in err.lower()


# --------------------------------------------------------------------------- worker parity


def test_worker_token_is_routed_to_the_store_backed_path(
    control_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same proof as ``test_control_backend.py``'s worker/admin tests: the test-only ASGI
    transport only covers :class:`~loomwork.remote.control.ControlClient` connections, so
    a worker token's store-backed fallback (``HttpStore``) tries a real socket and fails
    to connect -- evidence it took that branch rather than the control API."""
    token = control_env.tokens["worker"]
    code, _out, err = run(
        monkeypatch, token, "schedule", "create", "w-1", "sb.greet", '"a"', "--every", "1h"
    )
    assert code == ExitCode.UNAVAILABLE, err
    assert "cannot reach the server" in err


# --------------------------------------------------------------------------- action-level options


def test_operator_create_and_update_with_action_options(
    control_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = control_env.tokens["operator"]
    _code, out, _err = ok(
        monkeypatch,
        token,
        "schedule",
        "create",
        "vp-sched",
        "sb.greet",
        '"a"',
        "--every",
        "1h",
        "--versioning-policy",
        "pinned",
        "--id-reuse-policy",
        "reject_duplicate",
        "--run-timeout",
        "1h",
        "--attr",
        "tier=gold",
        "--json",
    )
    action = json.loads(out)["action"]
    assert action["versioning_policy"] == "pinned"
    assert action["id_reuse_policy"] == "reject_duplicate"
    assert action["run_timeout"] == 3600
    assert action["search_attributes"]["tier"] == {"type": "string", "value": "gold"}

    ok(
        monkeypatch,
        token,
        "schedule",
        "update",
        "vp-sched",
        "--id-conflict-policy",
        "terminate_existing",
        "--attr",
        "tier=silver",
    )
    _code, out, _err = ok(monkeypatch, token, "schedule", "describe", "vp-sched", "--json")
    action = json.loads(out)["action"]
    # Unset by the update: unchanged (the server merges each field with the current
    # action, unlike a fresh create).
    assert action["versioning_policy"] == "pinned"
    assert action["id_conflict_policy"] == "terminate_existing"
    assert action["search_attributes"] == {"tier": {"type": "string", "value": "silver"}}


def test_reader_cannot_update_action_options(
    control_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    operator = control_env.tokens["operator"]
    ok(monkeypatch, operator, "schedule", "create", "reader-vp", "sb.greet", '"a"', "--every", "1h")

    reader = control_env.tokens["reader"]
    code, _out, err = run(
        monkeypatch, reader, "schedule", "update", "reader-vp", "--versioning-policy", "pinned"
    )
    assert code != 0
    assert "control_write" in err or "forbidden" in err.lower()
