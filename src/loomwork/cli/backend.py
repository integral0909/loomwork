"""The operations backend: what ``workflow`` commands need, however the CLI connects.

``--store`` (or `--server` with a ``worker``/``admin`` token) uses the same
:class:`~loomwork.client.client.Client`/:class:`~loomwork.client.handle.WorkflowHandle`
objects the rest of Loomwork uses -- unchanged from before this module existed. A
``--server`` connection whose token only has the ``operator`` or ``reader`` role cannot
use those (they speak the worker API, see ``docs/server.md``), so it goes through
:class:`ControlOperations`/:class:`ControlWorkflowHandle` instead, talking to the
server's control API through :class:`~loomwork.remote.control.ControlClient`.

Both sides satisfy the same two structural protocols, :class:`Operations` and
:class:`OperationsHandle`, which mirror ``Client``'s and ``WorkflowHandle``'s own public
surface almost exactly (a real ``Client``/``WorkflowHandle`` needs no adapter at all --
they already have the right methods). Command handlers in
:mod:`loomwork.cli.commands.workflow` call this surface without caring which backend
produced it; :func:`open_operations` is what decides, once per invocation, which one to
build.

Reconstructing typed values from the control API's plain JSON (``execution_record_from_view``,
``event_from_view``) is what lets the exact same rendering code
(:mod:`loomwork.cli.commands.common`) serve both backends: the control API already
decodes every payload it can with the default converter, and these functions re-encode
that decoded value into a real :class:`~loomwork.codec.payload.Payload` (or, for a value
the server could not decode, reconstruct the original wire-form payload byte for byte),
so a reconstructed :class:`~loomwork.store.records.ExecutionRecord` or
:class:`~loomwork.history.events.Event` is indistinguishable from one loaded straight
from a store.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

from loomwork.cli.connection import ConnectionOptions, StoreUnavailableError, session
from loomwork.cli.errors import UsageError
from loomwork.client.client import BatchError, BatchResult
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.codec.payload import Payload
from loomwork.errors import (
    UpdateFailedError,
    WorkflowCanceledError,
    WorkflowClosedError,
    WorkflowFailedError,
    WorkflowTerminatedError,
    WorkflowTimedOutError,
)
from loomwork.failure import FailureInfo
from loomwork.history.events import Event
from loomwork.history.serde import event_from_dict
from loomwork.options import IdConflictPolicy, IdReusePolicy, ResetPoint
from loomwork.retry import RetryPolicy
from loomwork.store.records import ExecutionRecord, ExecutionStatus

if TYPE_CHECKING:
    # Both need the optional httpx dependency; the store-backed CLI works without it.
    import httpx

    from loomwork.remote.control import ControlClient

# --------------------------------------------------------------------------- reconstruction


def _payload_from_view(value: Any, converter: DataConverter) -> Payload | None:
    """The inverse of ``loomwork.server.responses.payload_view``."""
    if value is None:
        return None
    if isinstance(value, dict) and {"data", "metadata"} <= set(value):
        return Payload.from_dict(value)
    return converter.encode(value)


def _failure_from_view(data: dict[str, Any] | None) -> FailureInfo | None:
    """The inverse of ``loomwork.server.responses.failure_view``."""
    if data is None:
        return None
    return FailureInfo(
        message=str(data.get("message", "")),
        type=str(data.get("type", "")),
        kind=str(data.get("kind", "application")),
        non_retryable=bool(data.get("non_retryable", False)),
        details=data.get("details"),
        attributes=dict(data.get("attributes") or {}),
        cause=_failure_from_view(data.get("cause")),
    )


def execution_record_from_view(data: dict[str, Any], converter: DataConverter) -> ExecutionRecord:
    """The inverse of ``loomwork.server.responses.execution_view``.

    Fields the control API does not expose (``last_input_id``, ``consumed_through``,
    ``history_bytes``, ...) keep their dataclass defaults; nothing in the CLI reads them
    off a control-backed record.
    """
    closed_at = data.get("closed_at")
    return ExecutionRecord(
        run_id=str(data["run_id"]),
        workflow_id=str(data["workflow_id"]),
        workflow_name=str(data["workflow_name"]),
        status=ExecutionStatus(data["status"]),
        created_at=datetime.fromisoformat(data["created_at"]),
        closed_at=datetime.fromisoformat(closed_at) if closed_at else None,
        last_event_id=int(data.get("last_event_id", 0)),
        tick_pending=bool(data.get("tick_pending", False)),
        tick_failures=int(data.get("tick_failures", 0)),
        last_tick_error=data.get("last_tick_error"),
        lease_owner=data.get("lease_owner"),
        cancel_requested=bool(data.get("cancel_requested", False)),
        result=_payload_from_view(data.get("result"), converter),
        failure=_failure_from_view(data.get("failure")),
        close_reason=str(data.get("close_reason", "")),
        parent_run_id=data.get("parent_run_id"),
        parent_seq=data.get("parent_seq"),
        continued_run_id=data.get("continued_run_id"),
        reset_from_run_id=data.get("reset_from_run_id"),
    )


def event_from_view(data: dict[str, Any]) -> Event:
    """The inverse of ``loomwork.server.responses.event_view``.

    ``event_view`` starts from the event's lossless wire form
    (:func:`loomwork.history.serde.event_to_dict`) and only *replaces* a top-level
    ``result``/``payload`` field with ``{"payload": <original>, "value": <decoded>}``;
    undoing exactly that turns the JSON back into the same wire form
    :func:`~loomwork.history.serde.event_from_dict` already knows how to read, so every
    other field (including payloads under other names, such as ``args``, which
    ``event_view`` never touches) round-trips unchanged.
    """
    restored = dict(data)
    for key in ("result", "payload"):
        raw = restored.get(key)
        if isinstance(raw, dict) and set(raw) == {"payload", "value"}:
            restored[key] = raw["payload"]
    return event_from_dict(restored)


def _outcome_from_view(data: dict[str, Any], converter: DataConverter) -> Any:
    """The inverse of :meth:`~loomwork.client.handle.WorkflowHandle._outcome`, for a
    closed run's control-API view."""
    record = execution_record_from_view(data, converter)
    match record.status:
        case ExecutionStatus.COMPLETED:
            assert record.result is not None
            return converter.decode(record.result)
        case ExecutionStatus.FAILED:
            assert record.failure is not None
            error = WorkflowFailedError(record.run_id, record.failure)
            raise error from record.failure.to_exception()
        case ExecutionStatus.CANCELED:
            raise WorkflowCanceledError(record.run_id)
        case ExecutionStatus.TERMINATED:
            raise WorkflowTerminatedError(record.run_id, record.close_reason)
        case ExecutionStatus.TIMED_OUT:
            raise WorkflowTimedOutError(record.run_id, record.close_reason)
        case _:
            raise RuntimeError(f"run {record.run_id} is still {record.status}")


def _seconds(value: timedelta | float | None) -> float | None:
    if value is None:
        return None
    return value.total_seconds() if isinstance(value, timedelta) else float(value)


# --------------------------------------------------------------------------- protocols


class OperationsHandle(Protocol):
    """What a command needs from a handle to one run: the common surface of
    :class:`~loomwork.client.handle.WorkflowHandle` and :class:`ControlWorkflowHandle`."""

    workflow_id: str | None
    run_id: str | None

    async def describe(self) -> ExecutionRecord: ...
    async def history(self) -> list[Event]: ...
    async def children(self) -> list[ExecutionRecord]: ...
    async def search_attributes(self) -> dict[str, object]: ...
    async def signal(self, channel: str, value: Any, *, message_id: str | None = None) -> None: ...
    async def cancel(self, reason: str = "") -> bool: ...
    async def terminate(self, reason: str = "") -> bool: ...
    async def reset(self, *, to: ResetPoint, reason: str = "") -> OperationsHandle: ...
    async def result(self, timeout: float | None = None) -> Any: ...
    async def update(
        self,
        name: str,
        *args: Any,
        timeout: float | None = None,
        update_id: str | None = None,
    ) -> Any: ...


class Operations(Protocol):
    """What a command needs to start and find runs: the common surface of
    :class:`~loomwork.client.client.Client` and :class:`ControlOperations`."""

    converter: DataConverter

    async def start(
        self,
        workflow: str,
        *args: Any,
        id: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        run_timeout: timedelta | float | None = None,
        execution_timeout: timedelta | float | None = None,
        retry_policy: RetryPolicy | None = None,
        id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL,
    ) -> OperationsHandle: ...
    async def query_runs(self, query: str, limit: int = 100) -> list[ExecutionRecord]: ...
    # Named ``list`` last (matching ``Client``'s own ordering): once bound, that name
    # shadows the builtin for every annotation later in this class body, so nothing
    # after it may spell out a bare ``list[...]`` return type.
    async def list(
        self,
        *,
        workflow: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]: ...
    def get_handle(
        self, workflow_id: str | None = None, *, run_id: str | None = None
    ) -> OperationsHandle: ...
    async def batch_signal(
        self,
        query: str,
        channel: str,
        value: Any,
        *,
        message_id: str | None = None,
        page_size: int = 100,
        concurrency: int = 10,
    ) -> BatchResult: ...
    async def batch_cancel(
        self, query: str, reason: str = "", *, page_size: int = 100, concurrency: int = 10
    ) -> BatchResult: ...
    async def batch_terminate(
        self, query: str, reason: str = "", *, page_size: int = 100, concurrency: int = 10
    ) -> BatchResult: ...


# --------------------------------------------------------------------------- control-backed


class ControlWorkflowHandle:
    """:class:`OperationsHandle` backed by the control API."""

    def __init__(
        self,
        client: ControlClient,
        converter: DataConverter,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
    ) -> None:
        if workflow_id is None and run_id is None:
            raise ValueError("a handle needs a workflow_id or a run_id")
        self._client = client
        self._converter = converter
        self.workflow_id = workflow_id
        self.run_id = run_id

    def __repr__(self) -> str:
        return f"ControlWorkflowHandle(workflow_id={self.workflow_id!r}, run_id={self.run_id!r})"

    async def _describe_data(self) -> dict[str, Any]:
        if self.run_id is not None:
            return await self._client.describe_workflow(self.run_id)
        assert self.workflow_id is not None
        return await self._client.describe_workflow_by_id(self.workflow_id)

    async def _run_id(self) -> str:
        if self.run_id is not None:
            return self.run_id
        return str((await self._describe_data())["run_id"])

    async def describe(self) -> ExecutionRecord:
        return execution_record_from_view(await self._describe_data(), self._converter)

    async def history(self) -> list[Event]:
        events = await self._client.workflow_history(await self._run_id())
        return [event_from_view(e) for e in events]

    async def children(self) -> list[ExecutionRecord]:
        children = await self._client.workflow_children(await self._run_id())
        return [execution_record_from_view(c, self._converter) for c in children]

    async def search_attributes(self) -> dict[str, object]:
        data = await self._describe_data()
        attributes: dict[str, object] = dict(data.get("search_attributes") or {})
        return attributes

    async def signal(self, channel: str, value: Any, *, message_id: str | None = None) -> None:
        await self._client.signal_workflow(
            await self._run_id(), channel, value, message_id=message_id
        )

    async def cancel(self, reason: str = "") -> bool:
        return await self._client.cancel_workflow(await self._run_id(), reason)

    async def terminate(self, reason: str = "") -> bool:
        return await self._client.terminate_workflow(await self._run_id(), reason)

    async def reset(self, *, to: ResetPoint, reason: str = "") -> ControlWorkflowHandle:
        data = await self._client.reset_workflow(await self._run_id(), to=to, reason=reason)
        return ControlWorkflowHandle(
            self._client,
            self._converter,
            workflow_id=str(data["workflow_id"]),
            run_id=str(data["run_id"]),
        )

    async def result(self, timeout: float | None = None) -> Any:
        run_id = await self._run_id()
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            remaining = None if deadline is None else max(deadline - loop.time(), 0.0)
            data = await self._client.workflow_result(run_id, timeout=remaining)
            continued = data.get("continued_run_id")
            if continued:
                # Continued as new, or failed and retried: follow the chain, exactly like
                # WorkflowHandle.result -- the control API only ever answers for the one
                # run id given, never the chain it may have handed off to.
                run_id = str(continued)
                continue
            return _outcome_from_view(data, self._converter)

    async def update(
        self,
        name: str,
        *args: Any,
        timeout: float | None = None,
        update_id: str | None = None,
    ) -> Any:
        run_id = await self._run_id()
        resolved_update_id = await self._client.request_update(
            run_id, name, list(args), update_id=update_id
        )
        outcome = await self._client.wait_update(run_id, resolved_update_id, timeout=timeout)
        failure_data = outcome.get("failure")
        if failure_data is not None:
            failure = _failure_from_view(failure_data)
            assert failure is not None
            raise UpdateFailedError(resolved_update_id, failure) from failure.to_exception()
        return outcome.get("result")


class ControlOperations:
    """:class:`Operations` backed by the control API."""

    def __init__(self, client: ControlClient, converter: DataConverter | None = None) -> None:
        self._client = client
        self.converter = converter or default_converter

    async def start(
        self,
        workflow: str,
        *args: Any,
        id: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        run_timeout: timedelta | float | None = None,
        execution_timeout: timedelta | float | None = None,
        retry_policy: RetryPolicy | None = None,
        id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL,
    ) -> ControlWorkflowHandle:
        data = await self._client.start_workflow(
            workflow,
            list(args),
            workflow_id=id,
            search_attributes=search_attributes,
            run_timeout=_seconds(run_timeout),
            execution_timeout=_seconds(execution_timeout),
            retry_max_attempts=retry_policy.max_attempts if retry_policy is not None else None,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
        )
        return ControlWorkflowHandle(
            self._client,
            self.converter,
            workflow_id=str(data["workflow_id"]),
            run_id=str(data["run_id"]),
        )

    async def query_runs(self, query: str, limit: int = 100) -> list[ExecutionRecord]:
        data = await self._client.list_workflows(query=query, limit=limit)
        return [execution_record_from_view(r, self.converter) for r in data]

    # Named ``list`` last: see the matching comment on the ``Operations`` protocol.
    async def list(
        self,
        *,
        workflow: str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        data = await self._client.list_workflows(
            workflow_name=workflow,
            status=status.value if status is not None else None,
            limit=limit,
        )
        return [execution_record_from_view(r, self.converter) for r in data]

    def get_handle(
        self, workflow_id: str | None = None, *, run_id: str | None = None
    ) -> ControlWorkflowHandle:
        return ControlWorkflowHandle(
            self._client, self.converter, workflow_id=workflow_id, run_id=run_id
        )

    # ------------------------------------------------------------------ batch operations
    #
    # Mirrors Client._iter_matches/_batch (loomwork.client.client) closely: the control
    # API's ``query`` parameter accepts the same visibility-query text, including the
    # trailing ``ORDER BY``/``LIMIT`` a keyset page appends, so the same client-side
    # paging algorithm applies unchanged.

    async def _iter_matches(self, query: str, page_size: int) -> AsyncIterator[ExecutionRecord]:
        cursor: tuple[datetime, str] | None = None
        while True:
            page_query = query
            if cursor is not None:
                created_at, run_id = cursor
                ts, escaped_run_id = created_at.isoformat(), run_id.replace("'", "''")
                page_query = (
                    f"({query}) AND (created_at > timestamp '{ts}' "
                    f"OR (created_at = timestamp '{ts}' AND run_id > '{escaped_run_id}'))"
                )
            page_query += f" ORDER BY created_at ASC, run_id ASC LIMIT {page_size}"
            page = await self.query_runs(page_query, limit=page_size)
            if not page:
                return
            for record in page:
                yield record
            if len(page) < page_size:
                return
            last = page[-1]
            cursor = (last.created_at, last.run_id)

    async def _batch(
        self,
        query: str,
        apply: Callable[[str], Awaitable[bool]],
        *,
        page_size: int,
        concurrency: int,
    ) -> BatchResult:
        matched = 0
        succeeded = 0
        skipped = 0
        failed: list[BatchError] = []
        semaphore = asyncio.Semaphore(max(concurrency, 1))

        async def guarded(run_id: str) -> None:
            nonlocal succeeded, skipped
            try:
                try:
                    ok = await apply(run_id)
                except Exception as exc:
                    failed.append(BatchError(run_id, f"{type(exc).__name__}: {exc}"))
                    return
                if ok:
                    succeeded += 1
                else:
                    skipped += 1
            finally:
                semaphore.release()

        tasks: list[asyncio.Task[None]] = []
        async for record in self._iter_matches(query, page_size):
            matched += 1
            await semaphore.acquire()
            tasks.append(asyncio.ensure_future(guarded(record.run_id)))
        if tasks:
            await asyncio.gather(*tasks)
        return BatchResult(
            matched=matched, succeeded=succeeded, skipped_closed=skipped, failed=tuple(failed)
        )

    async def batch_signal(
        self,
        query: str,
        channel: str,
        value: Any,
        *,
        message_id: str | None = None,
        page_size: int = 100,
        concurrency: int = 10,
    ) -> BatchResult:
        async def apply(run_id: str) -> bool:
            try:
                await self._client.signal_workflow(run_id, channel, value, message_id=message_id)
            except WorkflowClosedError:
                return False
            return True

        return await self._batch(query, apply, page_size=page_size, concurrency=concurrency)

    async def batch_cancel(
        self, query: str, reason: str = "", *, page_size: int = 100, concurrency: int = 10
    ) -> BatchResult:
        async def apply(run_id: str) -> bool:
            return await self._client.cancel_workflow(run_id, reason)

        return await self._batch(query, apply, page_size=page_size, concurrency=concurrency)

    async def batch_terminate(
        self, query: str, reason: str = "", *, page_size: int = 100, concurrency: int = 10
    ) -> BatchResult:
        async def apply(run_id: str) -> bool:
            return await self._client.terminate_workflow(run_id, reason)

        return await self._batch(query, apply, page_size=page_size, concurrency=concurrency)


# --------------------------------------------------------------------------- backend selection


#: Test-only: route a ``--server`` base URL to an in-process ASGI transport instead of a
#: real socket, so CLI-over-control-API tests can drive a real server the same way every
#: other test in this repo does (``httpx.ASGITransport``) instead of binding a real port.
#: Never set outside tests; see ``tests/cli/test_control_backend.py``. This only affects
#: :class:`ControlClient` connections (the control-API path this module owns) -- the
#: store-backed fallback path (:func:`~loomwork.cli.connection.session`) is unchanged,
#: pre-existing code and needs no such hook.
_test_transports: dict[str, httpx.AsyncBaseTransport] = {}


@asynccontextmanager
async def open_operations(args: argparse.Namespace) -> AsyncIterator[Operations]:
    """Open whatever backend the ``workflow`` commands should use.

    ``--store`` always uses the local store, unchanged. ``--server`` probes the token's
    roles once (``GET .../control/whoami``): a ``worker`` or ``admin`` token gets the
    same store-backed path a remote worker uses (:class:`~loomwork.remote.http_store.
    HttpStore` under a :class:`~loomwork.client.client.Client`), so nothing changes for
    existing worker/admin setups; an ``operator`` or ``reader`` token -- which cannot use
    the worker API at all -- gets the control API instead. A token with neither is a
    clear usage error rather than a confusing 403 from whichever path was guessed.
    """
    options = ConnectionOptions.from_args(args)
    if options.server_url is None:
        async with session(options) as active:
            yield active.client
        return
    from loomwork.remote.control import ControlClient

    control = ControlClient(
        options.server_url,
        options.token,
        options.namespace,
        timeout=options.timeout,
        transport=_test_transports.get(options.server_url),
    )
    try:
        await control.open()
        who = await control.whoami()
    except OSError as exc:
        await control.close()
        raise StoreUnavailableError(f"cannot reach server {options.server_url}: {exc}") from None
    except BaseException:
        await control.close()
        raise
    if who.has_any("worker", "admin"):
        await control.close()
        async with session(options) as active:
            yield active.client
        return
    if not who.has_any("operator", "reader"):
        await control.close()
        raise UsageError(
            f"token {who.id!r} has no role usable for workflow commands "
            f"(has {', '.join(who.roles) or 'none'}; need worker, admin, operator or reader)"
        )
    try:
        yield ControlOperations(control, default_converter)
    finally:
        await control.close()


__all__ = [
    "ControlOperations",
    "ControlWorkflowHandle",
    "Operations",
    "OperationsHandle",
    "execution_record_from_view",
    "event_from_view",
    "open_operations",
]
