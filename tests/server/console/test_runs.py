"""Runs list and run detail: filters, the visibility query box, the history timeline,
relationships (children, continue-as-new), and the signal/cancel/terminate actions."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from loomwork.codec.converter import default_converter
from loomwork.sdk.definitions import Registry
from loomwork.server.app import create_app
from loomwork.server.auth import Role, TokenFile, generate_token
from loomwork.server.config import AuthConfig, ServerConfig
from loomwork.worker.worker import Worker, WorkerOptions
from tests.server.console.conftest import (
    ConsoleEnv,
    extract_csrf,
    login,
    start_echo,
    upper,
    wait_for_status,
)
from tests.server.console.conftest import echo_wf as echo_workflow

pytestmark = pytest.mark.integration


async def _login_operator(env: ConsoleEnv) -> None:
    await login(env.client, env.tokens.operator)


# --------------------------------------------------------------------------- runs list


async def test_runs_list_shows_started_run(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "list-1")
    await _login_operator(console_env)
    response = await console_env.client.get("/console/default/runs")
    assert response.status_code == 200
    assert record.workflow_id in response.text
    assert "console.echo" in response.text


async def test_runs_list_filters_by_status(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await start_echo(ns.engine, "list-status-1")
    await _login_operator(console_env)
    response = await console_env.client.get("/console/default/runs", params={"status": "failed"})
    assert response.status_code == 200
    assert "list-status-1" not in response.text


async def test_runs_list_filters_by_workflow_name(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await ns.engine.start_workflow("console.child", [default_converter.encode(1)])
    await start_echo(ns.engine, "list-wfname-1")
    await _login_operator(console_env)
    response = await console_env.client.get(
        "/console/default/runs", params={"workflow_name": "console.echo"}
    )
    assert response.status_code == 200
    assert "list-wfname-1" in response.text


async def test_runs_list_pagination_limit(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    for i in range(5):
        await start_echo(ns.engine, f"limit-{i}")
    await _login_operator(console_env)
    response = await console_env.client.get("/console/default/runs", params={"limit": "2"})
    assert response.status_code == 200
    assert response.text.count("limit-") <= 2 * 2  # workflow id appears in link text + href


async def test_runs_list_visibility_query(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "query-1")
    await _login_operator(console_env)
    response = await console_env.client.get(
        "/console/default/runs", params={"q": "status = 'running'"}
    )
    assert response.status_code == 200
    assert record.workflow_id in response.text


async def test_runs_list_invalid_query_shows_error_not_500(console_env: ConsoleEnv) -> None:
    await _login_operator(console_env)
    response = await console_env.client.get(
        "/console/default/runs", params={"q": "status = 'not-a-real-status'"}
    )
    assert response.status_code == 200
    assert "unknown status" in response.text.lower() or "status" in response.text.lower()

    syntax_error = await console_env.client.get(
        "/console/default/runs", params={"q": "status = = 'x'"}
    )
    assert syntax_error.status_code == 200


async def test_runs_list_escapes_hostile_workflow_id(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    hostile_id = "<script>alert('x')</script>"
    await start_echo(ns.engine, hostile_id)
    await _login_operator(console_env)
    response = await console_env.client.get("/console/default/runs")
    assert response.status_code == 200
    assert "<script>alert" not in response.text
    assert "&lt;script&gt;" in response.text


# --------------------------------------------------------------------------- run detail


async def test_run_detail_renders_summary_and_search_attributes(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await ns.engine.start_workflow(
        "console.echo",
        [default_converter.encode("hi")],
        workflow_id="detail-1",
        search_attributes={},
    )
    await _login_operator(console_env)
    response = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert response.status_code == 200
    assert "detail-1" in response.text
    assert "running" in response.text.lower()


async def test_run_detail_shows_history_and_pending_step_then_result(
    console_env: ConsoleEnv,
) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "detail-history-1")
    await _login_operator(console_env)

    # Wait for the worker to run the first tick (schedule + complete the step) before
    # asserting on it, rather than racing it.
    for _ in range(50):
        history = await ns.engine.history(record.run_id)
        if any(e.kind == "step_completed" for e in history):
            break
        await asyncio.sleep(0.02)

    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert page.status_code == 200
    assert "workflow_started" in page.text
    assert "step_scheduled" in page.text
    assert "step_completed" in page.text

    await ns.engine.signal(record.run_id, "msg", default_converter.encode("world"))
    await wait_for_status(ns.engine, record.run_id, "completed")

    done = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert done.status_code == 200
    assert "completed" in done.text.lower()
    assert "HI:world" in done.text


async def test_run_detail_shows_failure_placeholder_for_failed_run(
    console_env: ConsoleEnv,
) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await ns.engine.start_workflow("console.boom", [], workflow_id="detail-boom-1")
    await wait_for_status(ns.engine, record.run_id, "failed")
    await _login_operator(console_env)
    response = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert response.status_code == 200
    assert "kaboom" in response.text
    assert "Boom" in response.text


async def test_run_detail_shows_children_and_parent_link(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await ns.engine.start_workflow(
        "console.parent", [default_converter.encode(41)], workflow_id="detail-parent-1"
    )
    await wait_for_status(ns.engine, record.run_id, "completed")
    await _login_operator(console_env)

    parent_page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert parent_page.status_code == 200
    assert "Child:" in parent_page.text

    children = await ns.engine.children(record.run_id)
    assert len(children) == 1
    child_page = await console_env.client.get(f"/console/default/runs/{children[0].run_id}")
    assert child_page.status_code == 200
    assert "Parent:" in child_page.text
    assert record.run_id in child_page.text


async def test_run_detail_shows_continue_as_new_chain(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await ns.engine.start_workflow(
        "console.counter", [default_converter.encode(0)], workflow_id="detail-counter-1"
    )
    await wait_for_status(ns.engine, record.run_id, "continued_as_new")
    first = await ns.engine.describe(record.run_id)
    assert first.continued_run_id is not None
    await wait_for_status(ns.engine, first.continued_run_id, "continued_as_new")

    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    assert page.status_code == 200
    assert "Continued as:" in page.text
    assert first.continued_run_id in page.text

    next_page = await console_env.client.get(f"/console/default/runs/{first.continued_run_id}")
    assert next_page.status_code == 200
    assert "Continued from:" in next_page.text
    assert record.run_id in next_page.text


async def test_run_detail_unknown_run_is_404(console_env: ConsoleEnv) -> None:
    await _login_operator(console_env)
    response = await console_env.client.get("/console/default/runs/does-not-exist")
    assert response.status_code == 404


async def test_run_detail_unknown_namespace_is_404(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.admin)
    response = await console_env.client.get("/console/ghost-namespace/runs")
    assert response.status_code == 404


# --------------------------------------------------------------------------- actions


async def test_signal_action_delivers_and_flashes(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "signal-action-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/signal",
        data={"csrf_token": csrf, "channel": "msg", "value": '"world"'},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "flash=Signal" in response.headers["location"]

    await wait_for_status(ns.engine, record.run_id, "completed")
    result = await ns.engine.describe(record.run_id)
    assert default_converter.decode(result.result) == "HI:world"


async def test_signal_action_escapes_hostile_channel_and_value(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "signal-hostile-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    hostile_channel = "<script>chan</script>"
    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/signal",
        data={"csrf_token": csrf, "channel": hostile_channel, "value": '"<b>hi</b>"'},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert "<script>chan</script>" not in response.text
    assert "&lt;script&gt;chan&lt;/script&gt;" in response.text


async def test_signal_action_rejects_invalid_json_without_crashing(
    console_env: ConsoleEnv,
) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "signal-badjson-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/signal",
        data={"csrf_token": csrf, "channel": "msg", "value": "{not json"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "level=error" in response.headers["location"]


async def test_cancel_action_changes_state(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "cancel-action-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/cancel",
        data={"csrf_token": csrf, "reason": "operator says stop"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    updated = await ns.engine.describe(record.run_id)
    assert updated.cancel_requested is True


async def test_terminate_requires_confirmation(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "terminate-noconfirm-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/terminate",
        data={"csrf_token": csrf, "reason": "bye"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "level=error" in response.headers["location"]
    still_running = await ns.engine.describe(record.run_id)
    assert still_running.status.value == "running"


async def test_terminate_with_confirmation_closes_the_run(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "terminate-confirm-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/terminate",
        data={"csrf_token": csrf, "reason": "bye", "confirm": "yes"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    terminated = await ns.engine.describe(record.run_id)
    assert terminated.status.value == "terminated"
    assert terminated.close_reason == "bye"


async def test_actions_redirect_to_run_detail_with_flash_message(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    record = await start_echo(ns.engine, "flash-1")
    await _login_operator(console_env)
    page = await console_env.client.get(f"/console/default/runs/{record.run_id}")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        f"/console/default/runs/{record.run_id}/cancel",
        data={"csrf_token": csrf, "reason": "x"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert "Cancellation requested." in response.text


async def test_pending_step_job_is_shown(tmp_path: object) -> None:
    """A worker with ``step_concurrency=0`` runs ticks but never claims steps, so a
    scheduled step stays visible as a pending job -- exactly what an operator needs to
    see when a queue is stuck."""
    tokens_path = Path(str(tmp_path)) / "tokens.json"
    plaintext, record = generate_token("stuck-op", [Role.OPERATOR], ["stuck"])
    TokenFile({record.id: record}).save(tokens_path)

    config = ServerConfig(
        namespaces={"stuck": "memory://"}, auth=AuthConfig(tokens_file=tokens_path)
    )
    app = create_app(config, run_system_workers=False)
    async with app.router.lifespan_context(app):
        ns = app.state.server.namespace("stuck")
        registry = Registry(workflows=[echo_workflow], steps=[upper])
        worker = Worker(
            ns.store, registry, options=WorkerOptions(step_concurrency=0, shutdown_grace=1.0)
        )
        async with worker:
            started = await start_echo(ns.engine, "stuck-1")
            jobs = []
            for _ in range(50):
                jobs = await ns.engine.pending_steps(started.run_id)
                if jobs:
                    break
                await asyncio.sleep(0.02)
            assert jobs, "expected a pending step job"

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                await login(client, plaintext)
                page = await client.get(f"/console/stuck/runs/{started.run_id}")
                assert page.status_code == 200
                assert "Pending step jobs" in page.text
                assert "console.upper" in page.text
