"""Schedules list, schedule detail, and the pause/unpause/trigger/create/edit/delete/
backfill forms.

Creating and editing a schedule can fail on input a person typed by hand (a malformed
cron expression, an unknown IANA time zone, arguments that are not a JSON array): those
failures come back as :class:`~loomwork.schedules.model.ScheduleError` from
:class:`~loomwork.schedules.model.ScheduleSpec`/``SchedulePolicy`` themselves, and
:class:`_FormError` covers the console's own input checks (a missing workflow name,
non-numeric duration fields) the same way. Both are caught in the same place and re-render
the form with the submitted values intact and the message next to the field that needs
fixing, rather than the plain-text error page every other domain error in this console
gets (see ``errors.py``) -- a person filling in a form should not lose their work over a
typo.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from starlette.requests import Request
from starlette.responses import Response

from loomwork.codec.converter import default_converter
from loomwork.errors import ScheduleConflictError, ScheduleExistsError
from loomwork.options import IdConflictPolicy, IdReusePolicy, VersioningPolicy
from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    ScheduleError,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    validate_schedule_id,
)
from loomwork.schedules.service import ScheduleService
from loomwork.server.auth import require
from loomwork.server.console.render import redirect_with_flash, render
from loomwork.server.console.runtime import (
    ConsoleRuntime,
    accessible_namespaces,
    can_write,
    check_csrf,
    require_session,
)
from loomwork.server.console.util import parse_form, parse_int
from loomwork.visibility.attributes import decode_attributes, encode_attributes
from loomwork.visibility.schema import SchemaError

View = Callable[[Request], Awaitable[Response]]

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 500
_UPCOMING_COUNT = 5
_OVERLAP_POLICIES = [p.value for p in OverlapPolicy]
_ID_REUSE_POLICIES = [p.value for p in IdReusePolicy]
_ID_CONFLICT_POLICIES = [p.value for p in IdConflictPolicy]
_VERSIONING_POLICIES = [p.value for p in VersioningPolicy]


class _FormError(ValueError):
    """A schedule create/edit form field could not be understood."""


def _schedules_path(namespace: str) -> str:
    return f"/console/{namespace}/schedules"


def _schedule_path(namespace: str, schedule_id: str) -> str:
    return f"/console/{namespace}/schedules/{schedule_id}"


def _upcoming(spec: ScheduleSpec, now: datetime, count: int = _UPCOMING_COUNT) -> list[datetime]:
    times: list[datetime] = []
    current = now
    while len(times) < count:
        nominal = spec.next_after(current)
        if nominal is None:
            break
        times.append(nominal)
        current = nominal
    return times


def schedules_list_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = _schedules_path(namespace)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        limit = parse_int(
            request.query_params.get("limit"), default=_DEFAULT_LIMIT, minimum=1, maximum=_MAX_LIMIT
        )
        states = await ns.store.list_schedules(limit)
        return render(
            runtime,
            request,
            "schedules_list.html",
            namespace=namespace,
            namespaces=accessible_namespaces(runtime, session.record),
            states=states,
            limit=limit,
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


def schedule_detail_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        schedule_id = request.path_params["schedule_id"]
        path = _schedule_path(namespace, schedule_id)
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_read", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        state = await ns.store.get_schedule(schedule_id)
        upcoming = [] if state.paused else _upcoming(state.spec, ns.engine.clock.now())
        return render(
            runtime,
            request,
            "schedule_detail.html",
            namespace=namespace,
            state=state,
            upcoming=upcoming,
            can_write=can_write(session.record, namespace),
            csrf_token=session.csrf,
        )

    return view


async def _schedule_action(
    request: Request, runtime: ConsoleRuntime
) -> tuple[str, str, dict[str, str]]:
    namespace = request.path_params["namespace"]
    schedule_id = request.path_params["schedule_id"]
    path = _schedule_path(namespace, schedule_id)
    session = await require_session(request, runtime, next_path=path)
    require(session.record, capability="control_write", namespace=namespace)
    form = await parse_form(request)
    check_csrf(session, str(form.get("csrf_token", "")))
    return namespace, schedule_id, {str(k): str(v) for k, v in form.items()}


def schedule_pause_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        service = ScheduleService(ns.store, ns.engine.clock)
        await service.update(schedule_id, paused=True, note=form.get("note", ""))
        path = _schedule_path(namespace, schedule_id)
        return redirect_with_flash(path, level="success", message="Schedule paused.")

    return view


def schedule_unpause_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        service = ScheduleService(ns.store, ns.engine.clock)
        await service.update(schedule_id, paused=False, note=form.get("note", ""))
        path = _schedule_path(namespace, schedule_id)
        return redirect_with_flash(path, level="success", message="Schedule unpaused.")

    return view


def schedule_trigger_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, _form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        service = ScheduleService(ns.store, ns.engine.clock)
        state = await service.trigger(schedule_id)
        path = _schedule_path(namespace, schedule_id)
        latest = state.recent[-1] if state.recent else None
        message = (
            f"Started {latest.workflow_id} (run {latest.run_id})."
            if latest is not None
            else "Triggered."
        )
        return redirect_with_flash(path, level="success", message=message)

    return view


def schedule_backfill_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        path = _schedule_path(namespace, schedule_id)
        try:
            start = _parse_form_timestamp(form.get("start", ""), "Start")
            end = _parse_form_timestamp(form.get("end", ""), "End")
            if end <= start:
                raise _FormError("End must be after start.")
        except _FormError as exc:
            return redirect_with_flash(path, level="error", message=str(exc))
        service = ScheduleService(ns.store, ns.engine.clock)
        before = (await service.store.get_schedule(schedule_id)).total_started
        state = await service.backfill(schedule_id, start, end)
        started = state.total_started - before
        return redirect_with_flash(
            path, level="success", message=f"Backfill started {started} run(s)."
        )

    return view


# --------------------------------------------------------------------------- create / edit


def _parse_form_timestamp(text: str, label: str) -> datetime:
    text = text.strip()
    if not text:
        raise _FormError(f"{label} is required.")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise _FormError(f"{label} is not a valid date/time.") from None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _parse_form_float(form: dict[str, str], field: str, *, default: float, label: str) -> float:
    raw = form.get(field, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise _FormError(f"{label} must be a number.") from None


def _spec_from_form(form: dict[str, str]) -> ScheduleSpec:
    timezone = form.get("timezone", "").strip() or "UTC"
    jitter = _parse_form_float(form, "jitter_seconds", default=0.0, label="Jitter")
    try:
        if form.get("kind") == "interval":
            interval = _parse_form_float(form, "interval_seconds", default=0.0, label="Interval")
            if interval <= 0:
                raise _FormError("Interval must be a positive number of seconds.")
            return ScheduleSpec(interval=interval, timezone=timezone, jitter=jitter)
        cron = form.get("cron", "").strip()
        if not cron:
            raise _FormError("A cron expression is required.")
        return ScheduleSpec(cron=cron, timezone=timezone, jitter=jitter)
    except ScheduleError as exc:
        raise _FormError(str(exc)) from exc


def _policy_from_form(form: dict[str, str]) -> SchedulePolicy:
    catchup = _parse_form_float(
        form, "catchup_window_seconds", default=60.0, label="Catch-up window"
    )
    try:
        return SchedulePolicy(
            form.get("overlap", OverlapPolicy.SKIP.value),
            catchup,
            form.get("pause_on_failure") == "on",
        )
    except ScheduleError as exc:
        raise _FormError(str(exc)) from exc


def _args_from_form(form: dict[str, str]) -> list[Any]:
    text = form.get("args", "").strip()
    if not text:
        return []
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise _FormError(f"Arguments must be valid JSON: {exc.msg}.") from exc
    if not isinstance(value, list):
        raise _FormError('Arguments must be a JSON array, for example ["a", 2].')
    return value


def _search_attributes_from_form(form: dict[str, str]) -> dict[str, Any] | None:
    """``None`` when the field is blank (no change on edit, none set on create); a JSON
    object otherwise. Replaces the whole set, like ``loomwork schedule ... --attr``."""
    text = form.get("search_attributes", "").strip()
    if not text:
        return None
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise _FormError(f"Search attributes must be valid JSON: {exc.msg}.") from exc
    if not isinstance(value, dict):
        raise _FormError('Search attributes must be a JSON object, for example {"tier": "gold"}.')
    return value


def _optional_duration_from_form(
    form: dict[str, str], field: str, *, label: str
) -> timedelta | None:
    raw = form.get(field, "").strip()
    if not raw:
        return None
    try:
        seconds = float(raw)
    except ValueError:
        raise _FormError(f"{label} must be a number of seconds.") from None
    if seconds <= 0:
        raise _FormError(f"{label} must be positive.")
    return timedelta(seconds=seconds)


def _action_overrides_from_form(form: dict[str, str]) -> dict[str, Any]:
    """Action-level fields given in the form, as keyword arguments for
    :class:`~loomwork.schedules.model.ScheduleAction` -- only the ones the form actually
    set (a blank select or field means "leave alone" on edit, "use the default" on
    create)."""
    overrides: dict[str, Any] = {}
    if form.get("id_reuse_policy"):
        overrides["id_reuse_policy"] = IdReusePolicy(form["id_reuse_policy"])
    if form.get("id_conflict_policy"):
        overrides["id_conflict_policy"] = IdConflictPolicy(form["id_conflict_policy"])
    if form.get("versioning_policy"):
        overrides["versioning_policy"] = VersioningPolicy(form["versioning_policy"])
    run_timeout = _optional_duration_from_form(form, "run_timeout_seconds", label="Run timeout")
    if run_timeout is not None:
        overrides["run_timeout"] = run_timeout
    execution_timeout = _optional_duration_from_form(
        form, "execution_timeout_seconds", label="Execution timeout"
    )
    if execution_timeout is not None:
        overrides["execution_timeout"] = execution_timeout
    return overrides


def _action_to_form(action: ScheduleAction) -> dict[str, str]:
    return {
        "id_reuse_policy": action.id_reuse_policy.value if action.id_reuse_policy else "",
        "id_conflict_policy": (
            action.id_conflict_policy.value if action.id_conflict_policy else ""
        ),
        "versioning_policy": action.versioning_policy.value if action.versioning_policy else "",
        "run_timeout_seconds": (
            str(action.run_timeout.total_seconds()) if action.run_timeout else ""
        ),
        "execution_timeout_seconds": (
            str(action.execution_timeout.total_seconds()) if action.execution_timeout else ""
        ),
        "search_attributes": (
            json.dumps(decode_attributes(action.search_attributes), sort_keys=True)
            if action.search_attributes
            else ""
        ),
    }


def _spec_to_form(spec: ScheduleSpec) -> dict[str, str]:
    return {
        "kind": "interval" if spec.interval is not None else "cron",
        "cron": spec.cron or "",
        "interval_seconds": str(spec.interval.total_seconds()) if spec.interval else "",
        "timezone": spec.timezone,
        "jitter_seconds": str(spec.jitter.total_seconds()) if spec.jitter else "",
    }


def _policy_to_form(policy: SchedulePolicy) -> dict[str, str]:
    return {
        "overlap": policy.overlap.value,
        "catchup_window_seconds": str(policy.catchup_window.total_seconds()),
        "pause_on_failure": "on" if policy.pause_on_failure else "",
    }


def schedule_create_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        path = f"{_schedules_path(namespace)}/new"
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_write", namespace=namespace)
        ns = runtime.state.namespace(namespace)

        if request.method == "GET":
            return render(
                runtime,
                request,
                "schedule_create.html",
                namespace=namespace,
                overlaps=_OVERLAP_POLICIES,
                id_reuse_policies=_ID_REUSE_POLICIES,
                id_conflict_policies=_ID_CONFLICT_POLICIES,
                versioning_policies=_VERSIONING_POLICIES,
                form={"kind": "cron", "timezone": "UTC"},
                error=None,
                csrf_token=session.csrf,
            )

        form = await parse_form(request)
        check_csrf(session, form.get("csrf_token"))
        try:
            schedule_id = validate_schedule_id(form.get("schedule_id", "").strip())
            workflow = form.get("workflow", "").strip()
            if not workflow:
                raise _FormError("Workflow name is required.")
            args = _args_from_form(form)
            spec = _spec_from_form(form)
            policy = _policy_from_form(form)
            overrides = _action_overrides_from_form(form)
            raw_attributes = _search_attributes_from_form(form)
            encoded_args = await default_converter.encode_args_async(args)
            action = ScheduleAction(
                workflow,
                encoded_args,
                search_attributes=(
                    encode_attributes(raw_attributes, ns.engine.config.search_attributes)
                    if raw_attributes
                    else None
                ),
                **overrides,
            )
            state = ScheduleState(
                schedule_id=schedule_id,
                spec=spec,
                action=action,
                policy=policy,
                paused=form.get("paused") == "on",
                note=form.get("note", ""),
            )
            saved = await ScheduleService(ns.store, ns.engine.clock).create(state)
        except (_FormError, ScheduleError, SchemaError) as exc:
            return render(
                runtime,
                request,
                "schedule_create.html",
                status_code=422,
                namespace=namespace,
                overlaps=_OVERLAP_POLICIES,
                id_reuse_policies=_ID_REUSE_POLICIES,
                id_conflict_policies=_ID_CONFLICT_POLICIES,
                versioning_policies=_VERSIONING_POLICIES,
                form=form,
                error=str(exc),
                csrf_token=session.csrf,
            )
        except ScheduleExistsError as exc:
            return render(
                runtime,
                request,
                "schedule_create.html",
                status_code=409,
                namespace=namespace,
                overlaps=_OVERLAP_POLICIES,
                id_reuse_policies=_ID_REUSE_POLICIES,
                id_conflict_policies=_ID_CONFLICT_POLICIES,
                versioning_policies=_VERSIONING_POLICIES,
                form=form,
                error=str(exc),
                csrf_token=session.csrf,
            )
        return redirect_with_flash(
            _schedule_path(namespace, saved.schedule_id),
            level="success",
            message=f"Created schedule {saved.schedule_id}.",
        )

    return view


def schedule_edit_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        schedule_id = request.path_params["schedule_id"]
        path = f"{_schedule_path(namespace, schedule_id)}/edit"
        session = await require_session(request, runtime, next_path=path)
        require(session.record, capability="control_write", namespace=namespace)
        ns = runtime.state.namespace(namespace)
        service = ScheduleService(ns.store, ns.engine.clock)
        current = await service.store.get_schedule(schedule_id)

        if request.method == "GET":
            form = {
                **_spec_to_form(current.spec),
                **_policy_to_form(current.policy),
                **_action_to_form(current.action),
                "note": current.note,
            }
            return render(
                runtime,
                request,
                "schedule_edit.html",
                namespace=namespace,
                state=current,
                overlaps=_OVERLAP_POLICIES,
                id_reuse_policies=_ID_REUSE_POLICIES,
                id_conflict_policies=_ID_CONFLICT_POLICIES,
                versioning_policies=_VERSIONING_POLICIES,
                form=form,
                error=None,
                csrf_token=session.csrf,
            )

        form = await parse_form(request)
        check_csrf(session, form.get("csrf_token"))
        try:
            spec = _spec_from_form(form)
            policy = _policy_from_form(form)
            changes: dict[str, Any] = _action_overrides_from_form(form)
            raw_attributes = _search_attributes_from_form(form)
            if raw_attributes:
                changes["search_attributes"] = encode_attributes(
                    raw_attributes, ns.engine.config.search_attributes
                )
            update_kwargs: dict[str, Any] = {
                "spec": spec,
                "policy": policy,
                "note": form.get("note", ""),
            }
            if changes:
                # ScheduleService.update passes every keyword straight through to
                # dataclasses.replace, unlike ScheduleHandle.update -- an explicit
                # ``action=None`` here would wipe the schedule's action, so it is only
                # included at all when there is something to change.
                update_kwargs["action"] = dataclasses.replace(current.action, **changes)
            saved = await service.update(schedule_id, **update_kwargs)
        except (_FormError, ScheduleError, SchemaError) as exc:
            return render(
                runtime,
                request,
                "schedule_edit.html",
                status_code=422,
                namespace=namespace,
                state=current,
                overlaps=_OVERLAP_POLICIES,
                id_reuse_policies=_ID_REUSE_POLICIES,
                id_conflict_policies=_ID_CONFLICT_POLICIES,
                versioning_policies=_VERSIONING_POLICIES,
                form=form,
                error=str(exc),
                csrf_token=session.csrf,
            )
        except ScheduleConflictError as exc:
            return render(
                runtime,
                request,
                "schedule_edit.html",
                status_code=409,
                namespace=namespace,
                state=current,
                overlaps=_OVERLAP_POLICIES,
                id_reuse_policies=_ID_REUSE_POLICIES,
                id_conflict_policies=_ID_CONFLICT_POLICIES,
                versioning_policies=_VERSIONING_POLICIES,
                form=form,
                error=str(exc),
                csrf_token=session.csrf,
            )
        return redirect_with_flash(
            _schedule_path(namespace, saved.schedule_id), level="success", message="Saved."
        )

    return view


def schedule_delete_view(runtime: ConsoleRuntime) -> View:
    async def view(request: Request) -> Response:
        namespace, schedule_id, form = await _schedule_action(request, runtime)
        ns = runtime.state.namespace(namespace)
        if form.get("confirm") != "yes":
            return redirect_with_flash(
                _schedule_path(namespace, schedule_id),
                level="error",
                message="Confirm the checkbox to delete a schedule.",
            )
        deleted = await ns.store.delete_schedule(schedule_id)
        if not deleted:
            return redirect_with_flash(
                _schedule_path(namespace, schedule_id),
                level="warning",
                message="Schedule was already deleted.",
            )
        return redirect_with_flash(
            _schedules_path(namespace),
            level="success",
            message=f"Deleted schedule {schedule_id}. Runs it already started keep running.",
        )

    return view
