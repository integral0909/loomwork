"""Client API for starting and controlling workflows."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, TypeVar, overload

from loomwork.client.handle import WorkflowHandle
from loomwork.client.schedules import ScheduleClient
from loomwork.codec.converter import DataConverter, default_converter, return_hint
from loomwork.engine.engine import Engine
from loomwork.engine.queries import QueryRunner
from loomwork.errors import WorkflowClosedError
from loomwork.interceptors import ClientCallContext, ClientInterceptor, wrap_call
from loomwork.options import IdConflictPolicy, IdReusePolicy
from loomwork.retry import RetryPolicy
from loomwork.schedules.service import ScheduleService
from loomwork.sdk.definitions import Registry, WorkflowDefinition, WorkflowLike, as_definition
from loomwork.sdk.messaging import Channel, resolve_signal
from loomwork.store.records import ExecutionRecord, ExecutionStatus

logger = logging.getLogger("loomwork.client")

R = TypeVar("R")

_NO_HINT: Any = inspect.Parameter.empty

#: Default page size and bounded concurrency for ``Client.batch_*``.
_BATCH_PAGE_SIZE = 100
_BATCH_CONCURRENCY = 10


@dataclass(frozen=True, slots=True)
class BatchError:
    """One run a batch operation could not apply to."""

    run_id: str
    error: str


@dataclass(frozen=True, slots=True)
class BatchResult:
    """Summary of a ``Client.batch_*`` call.

    ``matched`` is the number of runs the query returned; every other run is one of
    ``succeeded`` (applied), ``skipped_closed`` (the run closed between matching and
    applying, so there was nothing to do), or counted in ``failed`` (an unexpected error;
    the operation was not applied to that run).
    """

    matched: int
    succeeded: int
    skipped_closed: int
    failed: tuple[BatchError, ...] = field(default_factory=tuple)


def _escape_literal(value: str) -> str:
    """Double single quotes for embedding ``value`` as a visibility query string literal."""
    return value.replace("'", "''")


class Client:
    """Starts workflows and obtains handles to them.

    Workflows can be referenced by definition (arguments are validated against its
    signature and results decoded to its return type) or by name (for workflows whose
    code is not importable by the caller).
    """

    def __init__(
        self,
        engine: Engine,
        converter: DataConverter | None = None,
        *,
        registry: Registry | None = None,
        interceptors: Sequence[ClientInterceptor] = (),
    ) -> None:
        self.engine = engine
        self.converter = converter or default_converter
        self.interceptors = interceptors
        self.queries = (
            QueryRunner(engine.store, registry, self.converter) if registry is not None else None
        )
        self.schedules = ScheduleClient(ScheduleService(engine.store, engine.clock), self.converter)
        self.versioning = engine.versioning

    @overload
    async def start(
        self,
        workflow: WorkflowDefinition[..., R],
        *args: Any,
        id: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        run_timeout: timedelta | float | None = None,
        execution_timeout: timedelta | float | None = None,
        retry_policy: RetryPolicy | None = None,
        id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL,
        headers: dict[str, str] | None = None,
    ) -> WorkflowHandle[R]: ...

    @overload
    async def start(
        self,
        workflow: str | type,
        *args: Any,
        id: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        run_timeout: timedelta | float | None = None,
        execution_timeout: timedelta | float | None = None,
        retry_policy: RetryPolicy | None = None,
        id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL,
        headers: dict[str, str] | None = None,
    ) -> WorkflowHandle[Any]: ...

    async def start(
        self,
        workflow: WorkflowLike | str,
        *args: Any,
        id: str | None = None,
        search_attributes: dict[str, Any] | None = None,
        run_timeout: timedelta | float | None = None,
        execution_timeout: timedelta | float | None = None,
        retry_policy: RetryPolicy | None = None,
        id_reuse_policy: IdReusePolicy = IdReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: IdConflictPolicy = IdConflictPolicy.FAIL,
        headers: dict[str, str] | None = None,
    ) -> WorkflowHandle[Any]:
        """Start a run and return a handle to it without waiting.

        ``id_reuse_policy`` and ``id_conflict_policy`` control what happens when ``id``
        has been used before; see :class:`~loomwork.options.IdReusePolicy` and
        :class:`~loomwork.options.IdConflictPolicy`. With ``IdConflictPolicy.
        USE_EXISTING`` the returned handle refers to the already-running execution.

        ``headers`` is recorded on the run's ``WorkflowStarted`` (see
        :meth:`~loomwork.engine.engine.Engine.start_workflow`); registered
        :class:`~loomwork.interceptors.ClientInterceptor` objects may add to it first,
        for example to propagate the caller's active trace.
        """
        if isinstance(workflow, str):
            name = workflow
            positional = list(args)
            hint = _NO_HINT
        else:
            definition = as_definition(workflow)
            name = definition.name
            positional = definition.bind(args)
            hint = return_hint(definition.fn)
        ctx = ClientCallContext(
            kind="start", workflow_name=name, workflow_id=id, headers=dict(headers or {})
        )
        with wrap_call(self.interceptors, ctx):
            record = await self.engine.start_workflow(
                name,
                await self.converter.encode_args_async(positional),
                workflow_id=id,
                search_attributes=search_attributes,
                run_timeout=run_timeout,
                execution_timeout=execution_timeout,
                retry_policy=retry_policy,
                id_reuse_policy=id_reuse_policy,
                id_conflict_policy=id_conflict_policy,
                headers=ctx.headers,
            )
        return WorkflowHandle(
            self.engine,
            self.converter,
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            result_hint=hint,
            queries=self.queries,
            interceptors=self.interceptors,
        )

    @overload
    async def execute(
        self,
        workflow: WorkflowDefinition[..., R],
        *args: Any,
        id: str | None = None,
        timeout: float | None = None,
    ) -> R: ...

    @overload
    async def execute(
        self, workflow: str | type, *args: Any, id: str | None = None, timeout: float | None = None
    ) -> Any: ...

    async def execute(
        self,
        workflow: WorkflowLike | str,
        *args: Any,
        id: str | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Start a run and wait for its result."""
        handle = await self.start(workflow, *args, id=id)
        return await handle.result(timeout=timeout)

    def get_handle(
        self,
        workflow_id: str | None = None,
        *,
        run_id: str | None = None,
        result_type: Any = _NO_HINT,
    ) -> WorkflowHandle[Any]:
        return WorkflowHandle(
            self.engine,
            self.converter,
            workflow_id=workflow_id,
            run_id=run_id,
            result_hint=result_type,
            queries=self.queries,
            interceptors=self.interceptors,
        )

    async def signal_with_start(
        self,
        workflow: WorkflowLike | str,
        *args: Any,
        id: str,
        channel: Channel[Any] | str | Callable[..., Any],
        value: Any,
        message_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> WorkflowHandle[Any]:
        """Send a message to workflow ``id``, starting it with ``args`` if not running.

        ``headers`` is recorded on ``WorkflowStarted`` when a new run is created (see
        :meth:`Client.start`); it has no effect when the workflow was already running.
        """
        if isinstance(workflow, str):
            name, positional, hint = workflow, list(args), _NO_HINT
        else:
            definition = as_definition(workflow)
            name, positional = definition.name, definition.bind(args)
            hint = return_hint(definition.fn)
        channel = resolve_signal(channel)
        if isinstance(channel, Channel):
            value = channel.validate(value)
            channel_name = channel.name
        else:
            channel_name = channel
        ctx = ClientCallContext(
            kind="signal_with_start",
            workflow_name=name,
            workflow_id=id,
            headers=dict(headers or {}),
        )
        with wrap_call(self.interceptors, ctx):
            record = await self.engine.signal_with_start(
                name,
                await self.converter.encode_args_async(positional),
                workflow_id=id,
                channel=channel_name,
                payload=await self.converter.encode_async(value),
                message_id=message_id or "",
                headers=ctx.headers,
            )
        return WorkflowHandle(
            self.engine,
            self.converter,
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            result_hint=hint,
            interceptors=self.interceptors,
            queries=self.queries,
        )

    async def query_runs(self, query: str, limit: int = 100) -> list[ExecutionRecord]:
        """Find runs with a visibility query, for example
        ``"status = 'failed' AND attr.customer_tier = 'gold' ORDER BY created_at DESC"``.
        See ``docs/visibility-queries.md``."""
        return await self.engine.query_executions(query, limit)

    async def list(
        self,
        *,
        workflow: WorkflowLike | str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        name = (
            workflow
            if workflow is None or isinstance(workflow, str)
            else as_definition(workflow).name
        )
        return await self.engine.list_executions(workflow_name=name, status=status, limit=limit)

    # ------------------------------------------------------------------ batch operations

    async def _iter_matches(self, query: str, page_size: int) -> AsyncIterator[ExecutionRecord]:
        """Page through every run matching ``query``, oldest first.

        ``query`` must be a plain filter: it may not carry its own ``ORDER BY`` or
        ``LIMIT``, since paging needs to append both (a keyset cursor on
        ``(created_at, run_id)``, which is stable even while matched runs are being
        closed or otherwise changed by earlier pages).
        """
        cursor: tuple[datetime, str] | None = None
        while True:
            page_query = query
            if cursor is not None:
                created_at, run_id = cursor
                ts, escaped_run_id = created_at.isoformat(), _escape_literal(run_id)
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
                    logger.warning("batch operation failed for run %s: %s", run_id, exc)
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
        channel: Channel[Any] | str | Callable[..., Any],
        value: Any,
        *,
        message_id: str | None = None,
        page_size: int = _BATCH_PAGE_SIZE,
        concurrency: int = _BATCH_CONCURRENCY,
    ) -> BatchResult:
        """Signal every run matching ``query`` (see :meth:`query_runs`).

        ``query`` must not carry its own ``ORDER BY``/``LIMIT``; see :meth:`_iter_matches`.
        A run that closes between being matched and being signalled counts as
        ``skipped_closed``, not a failure.
        """
        channel = resolve_signal(channel)
        if isinstance(channel, Channel):
            value = channel.validate(value)
            name = channel.name
        else:
            name = channel
        encoded = await self.converter.encode_async(value)

        async def apply(run_id: str) -> bool:
            try:
                await self.engine.signal(run_id, name, encoded, message_id=message_id or "")
            except WorkflowClosedError:
                return False
            return True

        return await self._batch(query, apply, page_size=page_size, concurrency=concurrency)

    async def batch_cancel(
        self,
        query: str,
        reason: str = "",
        *,
        page_size: int = _BATCH_PAGE_SIZE,
        concurrency: int = _BATCH_CONCURRENCY,
    ) -> BatchResult:
        """Request cancellation of every run matching ``query`` (see :meth:`query_runs`).

        A run already closed counts as ``skipped_closed``, not a failure.
        """

        async def apply(run_id: str) -> bool:
            return await self.engine.request_cancel(run_id, reason)

        return await self._batch(query, apply, page_size=page_size, concurrency=concurrency)

    async def batch_terminate(
        self,
        query: str,
        reason: str = "",
        *,
        page_size: int = _BATCH_PAGE_SIZE,
        concurrency: int = _BATCH_CONCURRENCY,
    ) -> BatchResult:
        """Terminate every run matching ``query`` (see :meth:`query_runs`).

        A run already closed counts as ``skipped_closed``, not a failure.
        """

        async def apply(run_id: str) -> bool:
            return await self.engine.terminate(run_id, reason)

        return await self._batch(query, apply, page_size=page_size, concurrency=concurrency)
