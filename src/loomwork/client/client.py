"""Client API for starting and controlling workflows."""

from __future__ import annotations

import inspect
from typing import Any, TypeVar, overload

from loomwork.client.handle import WorkflowHandle
from loomwork.codec.converter import DataConverter, default_converter, return_hint
from loomwork.engine.engine import Engine
from loomwork.sdk.definitions import WorkflowDefinition
from loomwork.store.records import ExecutionRecord, ExecutionStatus

R = TypeVar("R")

_NO_HINT: Any = inspect.Parameter.empty


class Client:
    """Starts workflows and obtains handles to them.

    Workflows can be referenced by definition (arguments are validated against its
    signature and results decoded to its return type) or by name (for workflows whose
    code is not importable by the caller).
    """

    def __init__(self, engine: Engine, converter: DataConverter | None = None) -> None:
        self.engine = engine
        self.converter = converter or default_converter

    @overload
    async def start(
        self, workflow: WorkflowDefinition[..., R], *args: Any, id: str | None = None
    ) -> WorkflowHandle[R]: ...

    @overload
    async def start(
        self, workflow: str, *args: Any, id: str | None = None
    ) -> WorkflowHandle[Any]: ...

    async def start(
        self,
        workflow: WorkflowDefinition[..., Any] | str,
        *args: Any,
        id: str | None = None,
    ) -> WorkflowHandle[Any]:
        """Start a run and return a handle to it without waiting."""
        if isinstance(workflow, WorkflowDefinition):
            name = workflow.name
            positional = workflow.bind(args)
            hint = return_hint(workflow.fn)
        else:
            name = workflow
            positional = list(args)
            hint = _NO_HINT
        record = await self.engine.start_workflow(
            name, self.converter.encode_args(positional), workflow_id=id
        )
        return WorkflowHandle(
            self.engine,
            self.converter,
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            result_hint=hint,
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
        self, workflow: str, *args: Any, id: str | None = None, timeout: float | None = None
    ) -> Any: ...

    async def execute(
        self,
        workflow: WorkflowDefinition[..., Any] | str,
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
        )

    async def list(
        self,
        *,
        workflow: WorkflowDefinition[..., Any] | str | None = None,
        status: ExecutionStatus | None = None,
        limit: int = 100,
    ) -> list[ExecutionRecord]:
        name = workflow.name if isinstance(workflow, WorkflowDefinition) else workflow
        return await self.engine.list_executions(workflow_name=name, status=status, limit=limit)
