"""Schedules list, schedule detail, and the pause/unpause/trigger actions."""

from __future__ import annotations

import pytest

from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
)
from tests.server.console.conftest import ConsoleEnv, extract_csrf, login

pytestmark = pytest.mark.integration


async def _create_schedule(env: ConsoleEnv, schedule_id: str, *, paused: bool = False) -> None:
    ns = env.app.state.server.namespace("default")
    state = ScheduleState(
        schedule_id=schedule_id,
        spec=ScheduleSpec(interval=3600, timezone="UTC"),
        action=ScheduleAction("console.child"),
        policy=SchedulePolicy(OverlapPolicy.SKIP),
        paused=paused,
    )
    from loomwork.schedules.service import ScheduleService

    await ScheduleService(ns.store, ns.engine.clock).create(state)


async def test_schedules_list_renders(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "nightly")
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/schedules")
    assert response.status_code == 200
    assert "nightly" in response.text
    assert "console.child" in response.text


async def test_schedule_detail_shows_spec_policy_and_upcoming(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "hourly")
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/schedules/hourly")
    assert response.status_code == 200
    assert "every" in response.text
    assert "skip" in response.text
    assert "Upcoming fire times" in response.text


async def test_schedule_detail_unknown_is_404(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/schedules/does-not-exist")
    assert response.status_code == 404


async def test_reader_cannot_pause_schedule(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "readonly-1")
    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.post(
        "/console/default/schedules/readonly-1/pause", data={"csrf_token": "x"}
    )
    assert response.status_code == 403


async def test_pause_and_unpause_change_persisted_state(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "toggle-1")
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/toggle-1")
    csrf = extract_csrf(page.text)

    paused_response = await console_env.client.post(
        "/console/default/schedules/toggle-1/pause",
        data={"csrf_token": csrf, "note": "maintenance"},
        follow_redirects=False,
    )
    assert paused_response.status_code == 303
    paused_state = await ns.store.get_schedule("toggle-1")
    assert paused_state.paused is True
    assert paused_state.note == "maintenance"

    page2 = await console_env.client.get("/console/default/schedules/toggle-1")
    csrf2 = extract_csrf(page2.text)
    unpaused_response = await console_env.client.post(
        "/console/default/schedules/toggle-1/unpause",
        data={"csrf_token": csrf2},
        follow_redirects=False,
    )
    assert unpaused_response.status_code == 303
    unpaused_state = await ns.store.get_schedule("toggle-1")
    assert unpaused_state.paused is False


async def test_trigger_starts_a_run(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "trigger-1")
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/trigger-1")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/schedules/trigger-1/trigger",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "flash=Started" in response.headers["location"]
    state = await ns.store.get_schedule("trigger-1")
    assert state.total_started == 1
    assert len(state.recent) == 1

    run_id = state.recent[0].run_id
    run_page = await console_env.client.get(f"/console/default/runs/{run_id}")
    assert run_page.status_code == 200
