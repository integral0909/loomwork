"""Answering workflow queries.

A query is answered by replaying the run's committed history into a fresh instance and
calling the registered handler. Nothing is written: queries see the state as of the last
committed tick, and they work on closed runs too (the state at completion).

Answering queries requires the workflow code, so a :class:`QueryRunner` needs a registry
containing the workflow definition.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from loomwork.codec.converter import DataConverter
from loomwork.codec.payload import Payload
from loomwork.codec.transform import CodecBoundary
from loomwork.errors import NotFoundError
from loomwork.history.events import WorkflowStarted
from loomwork.runtime.instance import WorkflowInstance
from loomwork.runtime.replayer import Replayer
from loomwork.sdk.definitions import Registry
from loomwork.store.base import Store


class QueryRunner:
    def __init__(self, store: Store, registry: Registry, converter: DataConverter) -> None:
        self.store = store
        self.registry = registry
        self.converter = converter

    def _make_instance(self, started: WorkflowStarted) -> WorkflowInstance:
        if not self.registry.has_workflow(started.workflow_name):
            raise NotFoundError(
                f"cannot answer queries for workflow {started.workflow_name!r}: its code is "
                "not registered in this process"
            )
        definition = self.registry.workflow(started.workflow_name)
        return WorkflowInstance(definition, started, converter=self.converter)

    async def query(self, run_id: str, name: str, args: Sequence[Payload]) -> Payload:
        history = await CodecBoundary(self.converter.codec).decode(
            await self.store.load_history(run_id)
        )
        replayer = Replayer(self._make_instance)
        try:
            replayer.feed(history)
            replayer.finish()
            instance = replayer.instance
            if instance is None:
                raise NotFoundError(f"run {run_id} has no history")
            return instance.run_query(name, args)
        finally:
            replayer.close()

    async def query_value(self, run_id: str, name: str, args: Sequence[Any], hint: Any) -> Any:
        payload = await self.query(run_id, name, self.converter.encode_args(list(args)))
        return self.converter.decode(payload, hint)
