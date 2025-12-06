"""Schedules list, schedule detail, and the pause/unpause/trigger actions."""

from __future__ import annotations

import pytest

from loomwork.errors import NotFoundError
from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
)
from loomwork.schedules.service import ScheduleService
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


# --------------------------------------------------------------------------- create


async def _new_schedule_csrf(env: ConsoleEnv) -> str:
    page = await env.client.get("/console/default/schedules/new")
    assert page.status_code == 200
    return extract_csrf(page.text)


_CRON_FORM = {
    "schedule_id": "created-1",
    "workflow": "console.child",
    "args": "[1]",
    "kind": "cron",
    "cron": "0 3 * * *",
    "timezone": "UTC",
    "jitter_seconds": "0",
    "overlap": "skip",
    "catchup_window_seconds": "60",
}


async def test_create_schedule_form_renders(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/schedules/new")
    assert response.status_code == 200
    assert "New schedule" in response.text


async def test_reader_cannot_see_or_submit_create_form(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.reader)
    get_response = await console_env.client.get("/console/default/schedules")
    assert "New schedule" not in get_response.text

    response = await console_env.client.get("/console/default/schedules/new")
    assert response.status_code == 403

    response = await console_env.client.post(
        "/console/default/schedules/new", data={**_CRON_FORM, "csrf_token": "x"}
    )
    assert response.status_code == 403


async def test_create_cron_schedule(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    csrf = await _new_schedule_csrf(console_env)

    response = await console_env.client.post(
        "/console/default/schedules/new",
        data={**_CRON_FORM, "csrf_token": csrf, "note": "reports"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].endswith(
        "/console/default/schedules/created-1?flash=Created+schedule+created-1.&level=success"
    )
    state = await ns.store.get_schedule("created-1")
    assert state.action.workflow_name == "console.child"
    assert state.spec.cron == "0 3 * * *"
    assert state.note == "reports"


async def test_create_interval_schedule(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    csrf = await _new_schedule_csrf(console_env)

    form = {**_CRON_FORM, "schedule_id": "created-interval", "kind": "interval"}
    form["interval_seconds"] = "1800"
    response = await console_env.client.post(
        "/console/default/schedules/new",
        data={**form, "csrf_token": csrf},
        follow_redirects=False,
    )
    assert response.status_code == 303
    state = await ns.store.get_schedule("created-interval")
    assert state.spec.interval is not None
    assert state.spec.interval.total_seconds() == 1800


async def test_create_rejects_missing_csrf(console_env: ConsoleEnv) -> None:
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.post("/console/default/schedules/new", data=_CRON_FORM)
    assert response.status_code == 403
    with pytest.raises(NotFoundError):
        await ns.store.get_schedule("created-1")


async def test_create_invalid_cron_shows_inline_error_and_keeps_values(
    console_env: ConsoleEnv,
) -> None:
    await login(console_env.client, console_env.tokens.operator)
    csrf = await _new_schedule_csrf(console_env)

    form = {**_CRON_FORM, "cron": "61 * * * *"}
    response = await console_env.client.post(
        "/console/default/schedules/new", data={**form, "csrf_token": csrf}
    )
    assert response.status_code == 422
    assert "form-error" in response.text
    # the submitted (invalid) values are kept in the re-rendered form, not lost
    assert 'value="61 * * * *"' in response.text
    assert 'value="created-1"' in response.text


async def test_create_unknown_timezone_shows_inline_error(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    csrf = await _new_schedule_csrf(console_env)

    form = {**_CRON_FORM, "timezone": "Not/AZone"}
    response = await console_env.client.post(
        "/console/default/schedules/new", data={**form, "csrf_token": csrf}
    )
    assert response.status_code == 422
    assert "form-error" in response.text
    assert "time zone" in response.text.lower()


async def test_create_invalid_json_args_shows_inline_error(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    csrf = await _new_schedule_csrf(console_env)

    form = {**_CRON_FORM, "args": "{not json"}
    response = await console_env.client.post(
        "/console/default/schedules/new", data={**form, "csrf_token": csrf}
    )
    assert response.status_code == 422
    assert "valid JSON" in response.text


async def test_create_duplicate_id_shows_inline_error(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "created-1")
    await login(console_env.client, console_env.tokens.operator)
    csrf = await _new_schedule_csrf(console_env)

    response = await console_env.client.post(
        "/console/default/schedules/new", data={**_CRON_FORM, "csrf_token": csrf}
    )
    assert response.status_code == 409
    assert "form-error" in response.text


async def test_create_escapes_hostile_note_on_error(console_env: ConsoleEnv) -> None:
    await login(console_env.client, console_env.tokens.operator)
    csrf = await _new_schedule_csrf(console_env)
    hostile = "<script>alert('note')</script>"

    form = {**_CRON_FORM, "cron": "not a cron", "note": hostile}
    response = await console_env.client.post(
        "/console/default/schedules/new", data={**form, "csrf_token": csrf}
    )
    assert response.status_code == 422
    assert "<script>alert" not in response.text
    assert "&lt;script&gt;" in response.text


# --------------------------------------------------------------------------- edit


async def test_edit_form_prefills_current_values(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "edit-1")
    await login(console_env.client, console_env.tokens.operator)
    response = await console_env.client.get("/console/default/schedules/edit-1/edit")
    assert response.status_code == 200
    assert 'value="3600.0"' in response.text
    assert 'value="skip"' in response.text or "selected" in response.text


async def test_reader_cannot_edit(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "edit-reader-1")
    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.get("/console/default/schedules/edit-reader-1/edit")
    assert response.status_code == 403
    response = await console_env.client.post(
        "/console/default/schedules/edit-reader-1/edit",
        data={"csrf_token": "x", "kind": "interval", "interval_seconds": "60"},
    )
    assert response.status_code == 403


async def test_edit_updates_policy_and_note(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "edit-2")
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/edit-2/edit")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/schedules/edit-2/edit",
        data={
            "csrf_token": csrf,
            "kind": "interval",
            "interval_seconds": "120",
            "timezone": "UTC",
            "jitter_seconds": "0",
            "overlap": "allow_all",
            "catchup_window_seconds": "10",
            "note": "edited",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    state = await ns.store.get_schedule("edit-2")
    assert state.spec.interval is not None and state.spec.interval.total_seconds() == 120
    assert state.policy.overlap.value == "allow_all"
    assert state.policy.catchup_window.total_seconds() == 10
    assert state.note == "edited"


async def test_edit_invalid_cron_shows_inline_error(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "edit-3")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/edit-3/edit")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/schedules/edit-3/edit",
        data={
            "csrf_token": csrf,
            "kind": "cron",
            "cron": "not a cron",
            "timezone": "UTC",
            "overlap": "skip",
            "catchup_window_seconds": "60",
        },
    )
    assert response.status_code == 422
    assert "form-error" in response.text


# --------------------------------------------------------------------------- delete


async def test_delete_requires_confirmation(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "delete-1")
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/delete-1")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/schedules/delete-1/delete",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "flash=Confirm" in response.headers["location"]
    # still there
    await ns.store.get_schedule("delete-1")


async def test_delete_removes_the_schedule(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "delete-2")
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/delete-2")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/schedules/delete-2/delete",
        data={"csrf_token": csrf, "confirm": "yes"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers["location"]
    assert "/console/default/schedules?" in location
    assert "/console/default/schedules/delete-2" not in location.split("?")[0]
    with pytest.raises(NotFoundError):
        await ns.store.get_schedule("delete-2")


async def test_reader_cannot_delete(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "delete-reader-1")
    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.post(
        "/console/default/schedules/delete-reader-1/delete",
        data={"csrf_token": "x", "confirm": "yes"},
    )
    assert response.status_code == 403


# --------------------------------------------------------------------------- backfill


async def test_backfill_starts_runs_for_the_range(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "backfill-1")
    ns = console_env.app.state.server.namespace("default")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/backfill-1")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/schedules/backfill-1/backfill",
        data={
            "csrf_token": csrf,
            "start": "2024-01-01T00:00",
            "end": "2024-01-01T03:00",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "flash=Backfill+started+3" in response.headers["location"]
    state = await ns.store.get_schedule("backfill-1")
    assert state.total_started == 3


async def test_backfill_rejects_end_before_start(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "backfill-2")
    await login(console_env.client, console_env.tokens.operator)
    page = await console_env.client.get("/console/default/schedules/backfill-2")
    csrf = extract_csrf(page.text)

    response = await console_env.client.post(
        "/console/default/schedules/backfill-2/backfill",
        data={
            "csrf_token": csrf,
            "start": "2024-01-02T00:00",
            "end": "2024-01-01T00:00",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "flash=End+must+be+after+start" in response.headers["location"]


async def test_reader_cannot_backfill(console_env: ConsoleEnv) -> None:
    await _create_schedule(console_env, "backfill-reader-1")
    await login(console_env.client, console_env.tokens.reader)
    response = await console_env.client.post(
        "/console/default/schedules/backfill-reader-1/backfill",
        data={"csrf_token": "x", "start": "2024-01-01T00:00", "end": "2024-01-01T03:00"},
    )
    assert response.status_code == 403
