"""Server-wide state: one store, engine and optional system worker per namespace.

Namespaces are how the server isolates tenants without separate schemas or deployments:
each namespace name maps to its own store URL (in-memory, SQLite or PostgreSQL), opened
once at startup and shared by every request for that namespace.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from loomwork.clock import Clock, SystemClock
from loomwork.engine.engine import Engine
from loomwork.errors import NotFoundError
from loomwork.sdk.definitions import Registry
from loomwork.server.auth import TokenFile
from loomwork.server.config import ServerConfig
from loomwork.store.base import Store
from loomwork.store.factory import redact_url, store_from_url
from loomwork.worker.worker import Worker, WorkerOptions

logger = logging.getLogger("loomwork.server")


@dataclass(slots=True)
class NamespaceState:
    """Everything the server needs to serve one namespace."""

    name: str
    store: Store
    engine: Engine
    system_worker: Worker | None

    async def open(self) -> None:
        await self.store.open()
        if self.system_worker is not None:
            await self.system_worker.start()

    async def close(self) -> None:
        if self.system_worker is not None:
            await self.system_worker.stop()
        await self.store.close()

    async def healthy(self) -> bool:
        try:
            await self.store.outbox_size()
            return True
        except Exception:
            logger.exception("namespace %r failed its readiness check", self.name)
            return False


class ServerState:
    """Constructed once by :func:`~loomwork.server.app.create_app` and stashed on
    ``app.state.server``."""

    def __init__(
        self,
        config: ServerConfig,
        *,
        clock: Clock | None = None,
        run_system_workers: bool = True,
    ) -> None:
        self.config = config
        self.clock = clock or SystemClock()
        self.run_system_workers = run_system_workers
        self.tokens: TokenFile | None = None
        self.namespaces: dict[str, NamespaceState] = {}
        if not config.auth.disabled:
            if config.auth.tokens_file is None:
                raise ValueError(
                    "server.auth.tokens_file is required unless server.auth.disabled = true"
                )
            self.tokens = TokenFile.load(config.auth.tokens_file)
        else:
            logger.warning(
                "loomwork server starting with auth.disabled = true: every request is "
                "treated as an administrator in every namespace. Never use this in "
                "production."
            )
        for name, url in config.namespaces.items():
            store = store_from_url(url)
            engine = Engine(store, clock=self.clock)
            worker = None
            if run_system_workers:
                worker = Worker(
                    store,
                    Registry(),
                    clock=self.clock,
                    options=WorkerOptions(
                        workflow_concurrency=0,
                        step_concurrency=0,
                        run_timers=True,
                        run_outbox=True,
                        run_schedules=True,
                    ),
                    worker_id=f"system:{name}",
                )
            self.namespaces[name] = NamespaceState(name, store, engine, worker)
            logger.info("namespace %r backed by %s", name, redact_url(url))

    def namespace(self, name: str) -> NamespaceState:
        try:
            return self.namespaces[name]
        except KeyError:
            raise NotFoundError(f"namespace {name!r} is not configured") from None

    async def open(self) -> None:
        for ns in self.namespaces.values():
            await ns.open()

    async def close(self) -> None:
        for ns in self.namespaces.values():
            await ns.close()
