"""Handles to workflow runs."""

from __future__ import annotations

import inspect
from typing import Any, Generic, TypeVar

from loomwork.codec.converter import DataConverter
from loomwork.engine.engine import Engine
from loomwork.errors import (
    WorkflowCanceledError,
    WorkflowFailedError,
    WorkflowTerminatedError,
)
from loomwork.history.events import Event
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
    ) -> None:
        if workflow_id is None and run_id is None:
            raise ValueError("a handle needs a workflow_id or a run_id")
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
