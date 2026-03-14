"""A thin async client for a Loomwork server's *control* API.

Unlike :class:`~loomwork.remote.http_store.HttpStore` (which speaks the worker API's
lossless wire form and needs a ``worker`` or ``admin`` token), :class:`ControlClient`
speaks the control API's plain JSON (see ``docs/server.md``): arguments, signal/update
values and results are ordinary JSON values, encoded and decoded server-side with the
default converter. It works with an ``operator`` or ``reader`` token, which is the point
of it -- it is what lets the :mod:`loomwork.cli` talk to a server on behalf of an operator
who was never handed worker credentials.

Every method returns the control API's own JSON shape (plain ``dict``/``list`` values,
already decoded where the server could decode them) rather than Loomwork's typed
``Store``/``Engine`` objects; :mod:`loomwork.cli.backend` is what turns those into the
same ``ExecutionRecord``/``Event`` objects the store-backed CLI path uses, so command
handlers do not need to know which one they got. Used directly (not through the CLI),
this client is also a reasonable way for a non-Python Python script to drive a Loomwork
server without importing the rest of the engine.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import httpx

from loomwork.diagnostics import DiagnosticsSnapshot
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint
from loomwork.remote.errors import ProblemDetail, exception_for

#: How long a single long-poll request (``/result``, update wait) may block before this
#: client re-issues it, when the caller asked for a longer (or unbounded) wait. Keeping
#: this under a server's likely ``limits.long_poll_max_seconds`` (default 20s) means a
#: single request rarely needs to be capped by the server instead of by us.
_POLL_CHUNK_SECONDS = 15.0

_RETRYABLE_METHODS = frozenset({"GET"})


def _backoff(attempt: int) -> float:
    return min(0.1 * (2.0**attempt), 2.0)


def _reset_point_to_wire(to: ResetPoint) -> dict[str, Any]:
    if to.kind == "after_tick":
        return {"after_tick": to.tick_count}
    return {"to": to.kind}


class WhoAmI:
    """The identity, roles, namespaces and server clock behind a bearer token, from
    ``GET .../control/whoami``."""

    __slots__ = ("id", "namespaces", "roles", "server_time")

    def __init__(
        self,
        id: str,
        roles: list[str],
        namespaces: list[str] | None = None,
        server_time: datetime | None = None,
    ) -> None:
        self.id = id
        self.roles = roles
        self.namespaces = namespaces or []
        self.server_time = server_time

    def has_any(self, *roles: str) -> bool:
        return any(role in self.roles for role in roles)


class ControlClient:
    """Talks to one namespace of a Loomwork server's control API.

    Args:
        base_url: the server's origin, for example ``"https://loomwork.internal"``.
        token: a bearer token with the ``operator`` or ``reader`` role (``admin`` also
            works, since it grants every capability).
        namespace: which of the server's namespaces to use.
        timeout: per-request timeout (seconds), excluding long-poll waits.
        max_retries: connection errors and ``5xx`` responses to idempotent (``GET``)
            requests are retried with jittered exponential backoff, up to this many
            extra attempts. Mirrors :class:`~loomwork.remote.http_store.HttpStore`.
        transport: an ``httpx`` transport override, used by tests to run against an
            in-process ASGI app (``httpx.ASGITransport``) instead of a real socket.
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        namespace: str = "default",
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.namespace = namespace
        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------ lifecycle

    async def open(self) -> None:
        if self._client is not None:
            return
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        self._client = httpx.AsyncClient(
            base_url=f"{self.base_url}/v1/namespaces/{self.namespace}/control",
            headers=headers,
            timeout=self.timeout,
            transport=self._transport,
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> ControlClient:
        await self.open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("ControlClient is not open; call await client.open() first")
        return self._client

    # ------------------------------------------------------------------ transport

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        idempotent = method in _RETRYABLE_METHODS
        attempt = 0
        while True:
            try:
                response = await self._http.request(method, path, json=json, params=params)
            except httpx.TransportError:
                if not idempotent or attempt >= self.max_retries:
                    raise
                await asyncio.sleep(_backoff(attempt))
                attempt += 1
                continue
            if response.status_code >= 500 and idempotent and attempt < self.max_retries:
                await asyncio.sleep(_backoff(attempt))
                attempt += 1
                continue
            if response.status_code >= 400:
                problem = ProblemDetail.from_wire(response.status_code, response.json())
                raise exception_for(problem)
            if response.status_code == 204 or not response.content:
                return None
            return response.json()

    # ------------------------------------------------------------------ identity

    async def whoami(self) -> WhoAmI:
        data = await self._request("GET", "/whoami")
        server_time = data.get("server_time")
        return WhoAmI(
            str(data["id"]),
            [str(r) for r in data["roles"]],
            [str(n) for n in data.get("namespaces", [])],
            datetime.fromisoformat(server_time) if server_time else None,
        )

    async def diagnostics(
        self,
        *,
        tick_failure_threshold: int | None = None,
        history_warning_fraction: float | None = None,
        queues: list[str] | None = None,
        limit: int | None = None,
    ) -> DiagnosticsSnapshot:
        """``GET .../control/diagnostics``: the facts behind ``loomwork doctor --server``
        (see :mod:`loomwork.diagnostics`). Omitted arguments use the server's defaults."""
        params: dict[str, Any] = {}
        if tick_failure_threshold is not None:
            params["tick_failure_threshold"] = tick_failure_threshold
        if history_warning_fraction is not None:
            params["history_warning_fraction"] = history_warning_fraction
        if queues:
            params["queue"] = queues
        if limit is not None:
            params["limit"] = limit
        data = await self._request("GET", "/diagnostics", params=params)
        return DiagnosticsSnapshot.from_dict(data)

    # ------------------------------------------------------------------ workflows

    async def start_workflow(
        self,
        name: str,
        args: list[Any],
        *,
        workflow_id: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        run_timeout: float | None = None,
        execution_timeout: float | None = None,
        retry_max_attempts: int | None = None,
        id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL,
        versioning_policy: str | None = None,
    ) -> dict[str, Any]:
        body = {
            "name": name,
            "args": args,
            "workflow_id": workflow_id,
            "search_attributes": search_attributes,
            "run_timeout": run_timeout,
            "execution_timeout": execution_timeout,
            "retry_max_attempts": retry_max_attempts,
            "id_reuse_policy": id_reuse_policy.value,
            "id_conflict_policy": id_conflict_policy.value,
            "versioning_policy": versioning_policy,
        }
        data: dict[str, Any] = await self._request("POST", "/workflows", json=body)
        return data

    async def signal_with_start(
        self,
        name: str,
        args: list[Any],
        *,
        workflow_id: str,
        channel: str,
        value: Any,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        body = {
            "name": name,
            "args": args,
            "workflow_id": workflow_id,
            "channel": channel,
            "value": value,
            "message_id": message_id or "",
        }
        data: dict[str, Any] = await self._request(
            "POST", "/workflows/signal-with-start", json=body
        )
        return data

    async def list_workflows(
        self,
        *,
        workflow_name: str | None = None,
        status: str | None = None,
        query: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if query is not None:
            params["query"] = query
        else:
            if workflow_name is not None:
                params["workflow_name"] = workflow_name
            if status is not None:
                params["status"] = status
        data = await self._request("GET", "/workflows", params=params)
        executions: list[dict[str, Any]] = data["executions"]
        return executions

    async def describe_workflow(self, run_id: str) -> dict[str, Any]:
        data: dict[str, Any] = await self._request("GET", f"/workflows/{run_id}")
        return data

    async def describe_workflow_by_id(self, workflow_id: str) -> dict[str, Any]:
        data: dict[str, Any] = await self._request("GET", f"/workflows/by-id/{workflow_id}")
        return data

    async def workflow_history(self, run_id: str) -> list[dict[str, Any]]:
        data = await self._request("GET", f"/workflows/{run_id}/history")
        events: list[dict[str, Any]] = data["events"]
        return events

    async def workflow_children(self, run_id: str) -> list[dict[str, Any]]:
        data = await self._request("GET", f"/workflows/{run_id}/children")
        children: list[dict[str, Any]] = data["children"]
        return children

    async def signal_workflow(
        self, run_id: str, channel: str, value: Any, *, message_id: str | None = None
    ) -> None:
        body = {"channel": channel, "value": value, "message_id": message_id or ""}
        await self._request("POST", f"/workflows/{run_id}/signal", json=body)

    async def request_update(
        self, run_id: str, name: str, args: list[Any], *, update_id: str | None = None
    ) -> str:
        body = {"name": name, "args": args, "update_id": update_id}
        data = await self._request("POST", f"/workflows/{run_id}/updates", json=body)
        return str(data["update_id"])

    async def wait_update(
        self, run_id: str, update_id: str, *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Long-polls for ``update_id``'s outcome, looping past the server's own
        long-poll cap so ``timeout=None`` means "wait indefinitely", matching
        :meth:`~loomwork.client.handle.WorkflowHandle.update`."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            remaining = None if deadline is None else max(deadline - loop.time(), 0.0)
            wait_for = (
                _POLL_CHUNK_SECONDS if remaining is None else min(remaining, _POLL_CHUNK_SECONDS)
            )
            try:
                data = await self._request(
                    "GET",
                    f"/workflows/{run_id}/updates/{update_id}",
                    params={"timeout": wait_for},
                )
            except TimeoutError:
                if deadline is not None and loop.time() >= deadline:
                    raise
                continue
            outcome: dict[str, Any] = data
            return outcome

    async def cancel_workflow(self, run_id: str, reason: str = "") -> bool:
        data = await self._request("POST", f"/workflows/{run_id}/cancel", json={"reason": reason})
        return bool(data["ok"])

    async def terminate_workflow(self, run_id: str, reason: str = "") -> bool:
        data = await self._request(
            "POST", f"/workflows/{run_id}/terminate", json={"reason": reason}
        )
        return bool(data["ok"])

    async def reset_workflow(
        self, run_id: str, *, to: ResetPoint, reason: str = ""
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"reason": reason, **_reset_point_to_wire(to)}
        data: dict[str, Any] = await self._request("POST", f"/workflows/{run_id}/reset", json=body)
        return data

    async def workflow_result(self, run_id: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Long-polls for ``run_id`` to close, looping past the server's own long-poll
        cap so ``timeout=None`` means "wait indefinitely" (see :meth:`wait_update`).

        Unlike :meth:`~loomwork.client.handle.WorkflowHandle.result`, this does not
        follow a ``continued_run_id`` chain to the run's true final outcome -- the
        control API answers for exactly the run id given. Callers that want the chain
        followed (the CLI does) do that themselves, one hop at a time.
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            remaining = None if deadline is None else max(deadline - loop.time(), 0.0)
            wait_for = (
                _POLL_CHUNK_SECONDS if remaining is None else min(remaining, _POLL_CHUNK_SECONDS)
            )
            try:
                data = await self._request(
                    "GET", f"/workflows/{run_id}/result", params={"timeout": wait_for}
                )
            except TimeoutError:
                if deadline is not None and loop.time() >= deadline:
                    raise
                continue
            record: dict[str, Any] = data
            return record

    async def repin_workflow(self, run_id: str, build_id: str | None) -> dict[str, Any]:
        data: dict[str, Any] = await self._request(
            "POST", f"/workflows/{run_id}/repin", json={"build_id": build_id}
        )
        return data

    # ------------------------------------------------------------------ builds

    async def builds_summary(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/builds")
        builds: list[dict[str, Any]] = data["builds"]
        return builds

    async def list_workers(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/builds/workers")
        workers: list[dict[str, Any]] = data["workers"]
        return workers

    async def list_build_compatibility(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/builds/compatibility")
        compatibility: list[dict[str, Any]] = data["compatibility"]
        return compatibility

    async def declare_build_compatible(self, build_id: str, compatible_with: str) -> None:
        await self._request(
            "POST",
            f"/builds/{build_id}/compatible",
            json={"compatible_with": compatible_with},
        )

    async def revoke_build_compatibility(self, build_id: str, compatible_with: str) -> bool:
        data = await self._request("DELETE", f"/builds/{build_id}/compatible/{compatible_with}")
        return bool(data["ok"])

    # ------------------------------------------------------------------ schedules

    async def create_schedule(
        self,
        schedule_id: str,
        workflow: str,
        args: list[Any],
        *,
        cron: str | None = None,
        interval: float | None = None,
        timezone: str = "UTC",
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        jitter: float = 0,
        overlap: str = "skip",
        catchup_window: float = 60,
        pause_on_failure: bool = False,
        paused: bool = False,
        note: str = "",
        id_reuse_policy: str | None = None,
        id_conflict_policy: str | None = None,
        versioning_policy: str | None = None,
        run_timeout: float | None = None,
        execution_timeout: float | None = None,
        search_attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = {
            "schedule_id": schedule_id,
            "workflow": workflow,
            "args": args,
            "cron": cron,
            "interval": interval,
            "timezone": timezone,
            "start_at": start_at.isoformat() if start_at else None,
            "end_at": end_at.isoformat() if end_at else None,
            "jitter": jitter,
            "overlap": overlap,
            "catchup_window": catchup_window,
            "pause_on_failure": pause_on_failure,
            "paused": paused,
            "note": note,
            "id_reuse_policy": id_reuse_policy,
            "id_conflict_policy": id_conflict_policy,
            "versioning_policy": versioning_policy,
            "run_timeout": run_timeout,
            "execution_timeout": execution_timeout,
            "search_attributes": search_attributes,
        }
        data: dict[str, Any] = await self._request("POST", "/schedules", json=body)
        return data

    async def list_schedules(self, limit: int = 100) -> list[dict[str, Any]]:
        data = await self._request("GET", "/schedules", params={"limit": limit})
        schedules: list[dict[str, Any]] = data["schedules"]
        return schedules

    async def describe_schedule(self, schedule_id: str) -> dict[str, Any]:
        data: dict[str, Any] = await self._request("GET", f"/schedules/{schedule_id}")
        return data

    async def pause_schedule(self, schedule_id: str, note: str = "") -> dict[str, Any]:
        data: dict[str, Any] = await self._request(
            "POST", f"/schedules/{schedule_id}/pause", json={"note": note}
        )
        return data

    async def unpause_schedule(self, schedule_id: str, note: str = "") -> dict[str, Any]:
        data: dict[str, Any] = await self._request(
            "POST", f"/schedules/{schedule_id}/unpause", json={"note": note}
        )
        return data

    async def trigger_schedule(self, schedule_id: str) -> dict[str, Any]:
        data: dict[str, Any] = await self._request("POST", f"/schedules/{schedule_id}/trigger")
        return data

    async def backfill_schedule(
        self, schedule_id: str, start: datetime, end: datetime
    ) -> dict[str, Any]:
        body = {"start": start.isoformat(), "end": end.isoformat()}
        data: dict[str, Any] = await self._request(
            "POST", f"/schedules/{schedule_id}/backfill", json=body
        )
        return data

    async def upcoming_schedule(self, schedule_id: str, count: int = 5) -> list[datetime]:
        data = await self._request(
            "GET", f"/schedules/{schedule_id}/upcoming", params={"count": count}
        )
        return [datetime.fromisoformat(t) for t in data["upcoming"]]

    async def update_schedule(
        self,
        schedule_id: str,
        *,
        cron: str | None = None,
        interval: float | None = None,
        timezone: str | None = None,
        jitter: float | None = None,
        overlap: str | None = None,
        catchup_window: float | None = None,
        pause_on_failure: bool | None = None,
        note: str | None = None,
        id_reuse_policy: str | None = None,
        id_conflict_policy: str | None = None,
        versioning_policy: str | None = None,
        run_timeout: float | None = None,
        execution_timeout: float | None = None,
        search_attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = {
            "cron": cron,
            "interval": interval,
            "timezone": timezone,
            "jitter": jitter,
            "overlap": overlap,
            "catchup_window": catchup_window,
            "pause_on_failure": pause_on_failure,
            "note": note,
            "id_reuse_policy": id_reuse_policy,
            "id_conflict_policy": id_conflict_policy,
            "versioning_policy": versioning_policy,
            "run_timeout": run_timeout,
            "execution_timeout": execution_timeout,
            "search_attributes": search_attributes,
        }
        data: dict[str, Any] = await self._request(
            "POST", f"/schedules/{schedule_id}/update", json=body
        )
        return data

    async def delete_schedule(self, schedule_id: str) -> bool:
        data = await self._request("DELETE", f"/schedules/{schedule_id}")
        return bool(data["ok"])


__all__ = ["ControlClient", "WhoAmI"]
