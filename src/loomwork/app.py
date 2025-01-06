"""Embedded mode: engine, client and worker in one object.

.. code-block:: python

    app = Loomwork(workflows=[process_order], steps=[charge_card, ship])
    async with app:
        receipt = await app.execute(process_order, order, id=f"order-{order.id}")
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, TypeVar, overload

from loomwork.client.client import Client
from loomwork.client.handle import WorkflowHandle
from loomwork.clock import Clock, SystemClock
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.engine.config import EngineConfig
from loomwork.engine.engine import Engine
from loomwork.errors import NotFoundError
from loomwork.sdk.definitions import Registry, StepDefinition, WorkflowDefinition
from loomwork.store.base import Store
from loomwork.store.memory import MemoryStore
from loomwork.worker.worker import Worker, WorkerOptions

R = TypeVar("R")


class Loomwork:
    """Hosts the engine and a worker inside the current process.

    Args:
        store: persistence backend; defaults to a non-durable :class:`MemoryStore`.
            A store passed in is opened if necessary but never closed: whoever creates
            a store owns its lifecycle, so several ``Loomwork`` objects can share one.
        workflows, steps: definitions this process can execute.
        clock: time source (use :class:`~loomwork.clock.ManualClock` in tests).
        run_worker: set to ``False`` to use this object purely as a client.
    """

    def __init__(
        self,
        *,
        store: Store | None = None,
        workflows: Iterable[WorkflowDefinition[..., Any]] = (),
        steps: Iterable[StepDefinition[..., Any]] = (),
        clock: Clock | None = None,
        converter: DataConverter | None = None,
        config: EngineConfig | None = None,
        worker_options: WorkerOptions | None = None,
        worker_id: str | None = None,
        run_worker: bool = True,
    ) -> None:
        self._owns_store = store is None
        self.store = store or MemoryStore()
        self.registry = Registry(workflows, steps)
        self.clock = clock or SystemClock()
        self.converter = converter or default_converter
        self.config = config or EngineConfig()
        self.engine = Engine(self.store, clock=self.clock, config=self.config)
        self.client = Client(self.engine, self.converter)
        self.worker = Worker(
            self.store,
            self.registry,
            clock=self.clock,
            converter=self.converter,
            config=self.config,
            options=worker_options,
            worker_id=worker_id,
        )
        self._run_worker = run_worker
        self._open = False

    # ------------------------------------------------------------------ registration

    def register(
        self, *definitions: WorkflowDefinition[..., Any] | StepDefinition[..., Any]
    ) -> None:
        """Register more definitions (allowed before and after starting)."""
        for definition in definitions:
            if isinstance(definition, WorkflowDefinition):
                self.registry.add_workflow(definition)
            else:
                self.registry.add_step(definition)

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        await self.store.open()
        self._open = True
        if self._run_worker:
            await self.worker.start()

    async def stop(self, grace: float | None = None) -> None:
        if self._run_worker:
            await self.worker.stop(grace)
        if self._open and self._owns_store:
            await self.store.close()
        self._open = False

    async def __aenter__(self) -> Loomwork:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    # ------------------------------------------------------------------ client shortcuts

    def _check_known(self, workflow: WorkflowDefinition[..., Any] | str) -> None:
        if isinstance(workflow, WorkflowDefinition) and not self.registry.has_workflow(
            workflow.name
        ):
            if not self._run_worker:
                return
            raise NotFoundError(
                f"workflow {workflow.name!r} is not registered with this Loomwork instance"
            )

    @overload
    async def start_workflow(
        self, workflow: WorkflowDefinition[..., R], *args: Any, id: str | None = None
    ) -> WorkflowHandle[R]: ...

    @overload
    async def start_workflow(
        self, workflow: str, *args: Any, id: str | None = None
    ) -> WorkflowHandle[Any]: ...

    async def start_workflow(
        self,
        workflow: WorkflowDefinition[..., Any] | str,
        *args: Any,
        id: str | None = None,
    ) -> WorkflowHandle[Any]:
        self._check_known(workflow)
        return await self.client.start(workflow, *args, id=id)

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
        self._check_known(workflow)
        return await self.client.execute(workflow, *args, id=id, timeout=timeout)

    def get_handle(
        self,
        workflow_id: str | None = None,
        *,
        run_id: str | None = None,
        result_type: Any = None,
    ) -> WorkflowHandle[Any]:
        if result_type is None:
            return self.client.get_handle(workflow_id, run_id=run_id)
        return self.client.get_handle(workflow_id, run_id=run_id, result_type=result_type)


__all__ = ["Loomwork"]
