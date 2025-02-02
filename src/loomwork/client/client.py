"""Client API for starting and controlling workflows."""

from __future__ import annotations

import inspect
from typing import Any, TypeVar, overload

from loomwork.client.handle import WorkflowHandle
from loomwork.codec.converter import DataConverter, default_converter, return_hint
from loomwork.engine.engine import Engine
from loomwork.engine.queries import QueryRunner
from loomwork.sdk.definitions import Registry, WorkflowDefinition
from loomwork.sdk.messaging import Channel
from loomwork.store.records import ExecutionRecord, ExecutionStatus

R = TypeVar("R")

_NO_HINT: Any = inspect.Parameter.empty


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
    ) -> None:
        self.engine = engine
        self.converter = converter or default_converter
        self.queries = (
            QueryRunner(engine.store, registry, self.converter) if registry is not None else None
        )

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
            queries=self.queries,
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
            queries=self.queries,
        )

    async def signal_with_start(
        self,
        workflow: WorkflowDefinition[..., Any] | str,
        *args: Any,
        id: str,
        channel: Channel[Any] | str,
        value: Any,
        message_id: str | None = None,
    ) -> WorkflowHandle[Any]:
        """Send a message to workflow ``id``, starting it with ``args`` if not running."""
        if isinstance(workflow, WorkflowDefinition):
            name, positional, hint = workflow.name, workflow.bind(args), return_hint(workflow.fn)
        else:
            name, positional, hint = workflow, list(args), _NO_HINT
        if isinstance(channel, Channel):
            value = channel.validate(value)
            channel_name = channel.name
        else:
            channel_name = channel
        record = await self.engine.signal_with_start(
            name,
            self.converter.encode_args(positional),
            workflow_id=id,
            channel=channel_name,
            payload=self.converter.encode(value),
            message_id=message_id or "",
        )
        return WorkflowHandle(
            self.engine,
            self.converter,
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            result_hint=hint,
            queries=self.queries,
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
