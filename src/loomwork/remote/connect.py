"""Convenience constructors for talking to a Loomwork server.

.. code-block:: python

    async with await connect("https://loomwork.internal", token, namespace="billing") as store:
        engine = Engine(store)
        client = Client(engine, registry=my_registry)
        handle = await client.start(process_order, order, id=f"order-{order.id}")

        worker = Worker(store, my_registry)
        async with worker:
            ...
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loomwork.clock import Clock
from loomwork.codec.converter import DataConverter
from loomwork.engine.config import EngineConfig
from loomwork.engine.engine import Engine
from loomwork.remote.http_store import HttpStore
from loomwork.sdk.definitions import Registry

if TYPE_CHECKING:
    from loomwork.client.client import Client


async def connect(
    base_url: str,
    token: str | None = None,
    namespace: str = "default",
    *,
    timeout: float = 30.0,
    long_poll: float = 20.0,
    clock: Clock | None = None,
) -> HttpStore:
    """Open an :class:`~loomwork.remote.http_store.HttpStore` connected to a server.

    The returned store is already open (``await store.open()`` has run); close it with
    ``await store.close()`` or ``async with``.
    """
    store = HttpStore(base_url, token, namespace, timeout=timeout, long_poll=long_poll, clock=clock)
    await store.open()
    return store


def engine_for(
    store: HttpStore, *, clock: Clock | None = None, config: EngineConfig | None = None
) -> Engine:
    """Build an :class:`~loomwork.engine.engine.Engine` over an already-open store."""
    return Engine(store, clock=clock or store.clock, config=config)


def client_for(
    store: HttpStore,
    *,
    registry: Registry | None = None,
    converter: DataConverter | None = None,
    clock: Clock | None = None,
    config: EngineConfig | None = None,
) -> Client:
    """Build a :class:`~loomwork.client.client.Client` over an already-open store."""
    from loomwork.client.client import Client

    return Client(engine_for(store, clock=clock, config=config), converter, registry=registry)
