"""Handles to workflow runs."""

from __future__ import annotations

import inspect
from typing import Any, Generic, TypeVar

from loomwork.codec.converter import DataConverter
from loomwork.engine.engine import Engine
from loomwork.engine.queries import QueryRunner
from loomwork.errors import (
    LoomworkError,
    UpdateFailedError,
    WorkflowCanceledError,
    WorkflowFailedError,
    WorkflowTerminatedError,
)
from loomwork.history.events import Event
from loomwork.sdk.messaging import Channel
from loomwork.store.records import ExecutionRecord, ExecutionStatus, StepJob

R = TypeVar("R")

_NO_HINT: Any = inspect.Parameter.empty


class WorkflowHandle(Generic[R]):
    """Refers to one workflow run.

    A handle created from a workflow id alone follows the *latest* run of that id at the
    time of each call.
    """

    def __init__(
        self,
        engine: Engine,
        converter: DataConverter,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
        result_hint: Any = _NO_HINT,
        queries: QueryRunner | None = None,
    ) -> None:
        if workflow_id is None and run_id is None:
            raise ValueError("a handle needs a workflow_id or a run_id")
        self._queries = queries
        self._engine = engine
        self._converter = converter
        self.workflow_id = workflow_id
        self.run_id = run_id
        self._result_hint = result_hint

    def __repr__(self) -> str:
        return f"WorkflowHandle(workflow_id={self.workflow_id!r}, run_id={self.run_id!r})"

    async def _run_id(self) -> str:
        if self.run_id is not None:
            return self.run_id
        record = await self._engine.resolve(workflow_id=self.workflow_id)
        return record.run_id

    async def describe(self) -> ExecutionRecord:
        return await self._engine.resolve(workflow_id=self.workflow_id, run_id=self.run_id)

    async def history(self) -> list[Event]:
        return await self._engine.history(await self._run_id())

    async def pending_steps(self) -> list[StepJob]:
        return await self._engine.pending_steps(await self._run_id())

    async def children(self) -> list[ExecutionRecord]:
        return await self._engine.children(await self._run_id())

    async def signal(
        self, channel: Channel[Any] | str, value: Any, *, message_id: str | None = None
    ) -> None:
        """Send ``value`` to one of the workflow's channels.

        When a :class:`~loomwork.Channel` is given, the value is validated against its
        type first. ``message_id`` makes delivery idempotent: the workflow ignores a
        second message with the same id.
        """
        if isinstance(channel, Channel):
            value = channel.validate(value)
            name = channel.name
        else:
            name = channel
        await self._engine.signal(
            await self._run_id(),
            name,
            self._converter.encode(value),
            message_id=message_id or "",
        )

    async def query(self, name: str, *args: Any, result_type: Any = _NO_HINT) -> Any:
        """Ask the workflow a read-only question (requires its code in this process)."""
        if self._queries is None:
            raise LoomworkError("queries need the workflow code: create the client with a registry")
        return await self._queries.query_value(await self._run_id(), name, args, result_type)

    async def update(
        self,
        name: str,
        *args: Any,
        result_type: Any = _NO_HINT,
        timeout: float | None = None,
        update_id: str | None = None,
    ) -> Any:
        """Run update handler ``name`` in the workflow and return its result.

        Raises:
            UpdateFailedError: the handler raised.
            WorkflowClosedError: the run closed before handling the update.
            TimeoutError: no outcome within ``timeout`` seconds (the update may still
                run later; retry with the same ``update_id`` to wait again).
        """
        run_id = await self._run_id()
        update_id = await self._engine.request_update(
            run_id, name, self._converter.encode_args(list(args)), update_id=update_id
        )
        outcome = await self._engine.wait_update(run_id, update_id, timeout)
        if outcome.failure is not None:
            raise UpdateFailedError(update_id, outcome.failure) from outcome.failure.to_exception()
        assert outcome.result is not None
        return self._converter.decode(outcome.result, result_type)

    async def cancel(self, reason: str = "") -> bool:
        return await self._engine.request_cancel(await self._run_id(), reason)

    async def terminate(self, reason: str = "") -> bool:
        return await self._engine.terminate(await self._run_id(), reason)

    async def result(self, timeout: float | None = None) -> R:
        """Wait for the run to close and return its result.

        Raises:
            WorkflowFailedError: the run failed; ``.failure`` holds the details.
            WorkflowCanceledError: the run was cancelled.
            WorkflowTerminatedError: the run was terminated.
            TimeoutError: the run did not close within ``timeout`` seconds.
        """
        run_id = await self._run_id()
        record = await self._engine.wait_closed(run_id, timeout)
        return self._outcome(record)

    def _outcome(self, record: ExecutionRecord) -> R:
        match record.status:
            case ExecutionStatus.COMPLETED:
                assert record.result is not None
                value: R = self._converter.decode(record.result, self._result_hint)
                return value
            case ExecutionStatus.FAILED:
                assert record.failure is not None
                error = WorkflowFailedError(record.run_id, record.failure)
                raise error from record.failure.to_exception()
            case ExecutionStatus.CANCELED:
                raise WorkflowCanceledError(record.run_id)
            case ExecutionStatus.TERMINATED:
                raise WorkflowTerminatedError(record.run_id, record.close_reason)
            case _:
                raise RuntimeError(f"run {record.run_id} is still {record.status}")
