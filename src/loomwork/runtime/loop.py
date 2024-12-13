"""A deterministic :class:`asyncio.AbstractEventLoop` for workflow code.

The loop never blocks and never talks to the operating system. The replay executor drives
it explicitly with :meth:`DeterministicEventLoop.run_until_idle`, which runs ready
callbacks in FIFO order until none are left.

Time is *workflow time*: :meth:`time` returns the current tick's timestamp as Unix
seconds. Scheduling a callback in the future (``call_at`` / ``call_later``, and therefore
``asyncio.sleep``, ``asyncio.timeout`` and ``asyncio.wait_for``) is forwarded to a
:class:`TimerBridge`, which turns it into a durable timer. When the timer fires, the
bridge hands the handle back through :meth:`DeterministicEventLoop.fire_timer`.

Anything that would perform I/O or depend on the host (executors, sockets, subprocesses,
signal handlers, thread-safe callbacks) raises :class:`DeterminismViolation`. The
violation is also recorded on the loop so that the executor fails the tick even if the
workflow code swallows the exception.
"""

from __future__ import annotations

import asyncio
import collections
import contextvars
import logging
from collections.abc import Callable, Coroutine, Generator
from typing import Any, Protocol, TypeVar, cast

from loomwork.errors import DeterminismViolation

_T = TypeVar("_T")

logger = logging.getLogger("loomwork.runtime")


def _set_scheduled(handle: asyncio.TimerHandle, value: bool) -> None:
    # ``TimerHandle._scheduled`` is how BaseEventLoop tracks heap membership; it is not
    # in the type stubs. We use it with the same meaning: "a durable timer exists".
    setattr(handle, "_scheduled", value)  # noqa: B010


class TimerBridge(Protocol):
    """Receives timer requests from a :class:`DeterministicEventLoop`."""

    def start_timer(self, handle: asyncio.TimerHandle) -> None:
        """A callback was scheduled for ``handle.when()`` (in the future)."""
        ...

    def cancel_timer(self, handle: asyncio.TimerHandle) -> None:
        """A previously started timer handle was cancelled."""
        ...


class DeterministicEventLoop(asyncio.AbstractEventLoop):
    """Event loop that executes workflow coroutines deterministically."""

    def __init__(self, bridge: TimerBridge, *, start_time: float = 0.0) -> None:
        self._bridge = bridge
        self._ready: collections.deque[asyncio.Handle] = collections.deque()
        self._time = start_time
        self._closed = False
        self._running = False
        self._task_counter = 0
        self._exception_handler: (
            Callable[[asyncio.AbstractEventLoop, dict[str, Any]], object] | None
        ) = None
        self._task_factory: Any = None
        self._debug = False
        self._violation: DeterminismViolation | None = None
        #: The object driving this loop (the workflow instance); used by SDK functions
        #: to find their execution context.
        self.owner: Any = None
        #: Exception contexts reported through ``call_exception_handler`` (for example
        #: a background task that failed and was never awaited).
        self.unhandled_errors: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ driving

    def set_time(self, value: float) -> None:
        """Set workflow time. Time never moves backwards."""
        if value < self._time:
            raise ValueError("workflow time cannot move backwards")
        self._time = value

    def run_until_idle(self) -> None:
        """Run ready callbacks until the queue is empty.

        Raises:
            DeterminismViolation: if workflow code attempted a forbidden operation
                during this run, even if the code caught the resulting exception.
        """
        if self._closed:
            raise RuntimeError("event loop is closed")
        if self._running:
            raise RuntimeError("run_until_idle() is not re-entrant")
        previous = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(self)
        self._running = True
        try:
            while self._ready:
                handle = self._ready.popleft()
                if handle.cancelled():
                    continue
                handle._run()
                if self._violation is not None:
                    break
        finally:
            self._running = False
            asyncio.events._set_running_loop(previous)
        violation, self._violation = self._violation, None
        if violation is not None:
            raise violation

    def fire_timer(self, handle: asyncio.TimerHandle) -> None:
        """Queue a timer handle whose durable timer has fired."""
        # A fired timer can no longer be cancelled durably; cancelling the handle
        # afterwards (as asyncio.timeout() does on exit) must not emit a command.
        _set_scheduled(handle, False)
        if not handle.cancelled():
            self._ready.append(handle)

    @property
    def has_ready(self) -> bool:
        return bool(self._ready)

    def _forbid(self, operation: str) -> DeterminismViolation:
        violation = DeterminismViolation(
            f"{operation} is not allowed in workflow code; move it into a step"
        )
        if self._violation is None:
            self._violation = violation
        return violation

    # ------------------------------------------------------------------ scheduling

    def time(self) -> float:
        return self._time

    def call_soon(  # type: ignore[override]
        self,
        callback: Callable[..., object],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.Handle:
        self._check_closed()
        handle = asyncio.Handle(callback, args, self, context)
        self._ready.append(handle)
        return handle

    def call_later(  # type: ignore[override]
        self,
        delay: float,
        callback: Callable[..., object],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        return self.call_at(self._time + max(delay, 0.0), callback, *args, context=context)

    def call_at(  # type: ignore[override]
        self,
        when: float,
        callback: Callable[..., object],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        self._check_closed()
        handle = asyncio.TimerHandle(when, callback, args, self, context)
        if when <= self._time:
            self._ready.append(handle)
        else:
            _set_scheduled(handle, True)
            self._bridge.start_timer(handle)
        return handle

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        # Called by TimerHandle.cancel() before the handle is marked cancelled.
        if getattr(handle, "_scheduled", False):
            _set_scheduled(handle, False)
            self._bridge.cancel_timer(handle)

    def call_soon_threadsafe(  # type: ignore[override]
        self,
        callback: Callable[..., object],
        *args: Any,
        context: contextvars.Context | None = None,
    ) -> asyncio.Handle:
        raise self._forbid("call_soon_threadsafe()")

    # ------------------------------------------------------------------ futures and tasks

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(
        self,
        coro: Coroutine[Any, Any, _T] | Generator[Any, None, _T],
        *,
        name: str | None = None,
        context: contextvars.Context | None = None,
    ) -> asyncio.Task[_T]:
        self._check_closed()
        self._task_counter += 1
        if name is None:
            # Deterministic names; the default global counter differs between processes.
            name = f"workflow-task-{self._task_counter}"
        if self._task_factory is not None:
            if context is None:
                task = self._task_factory(self, coro)
            else:
                task = self._task_factory(self, coro, context=context)
            task.set_name(name)
            return task  # type: ignore[no-any-return]
        return asyncio.Task(
            cast(Coroutine[Any, Any, _T], coro), loop=self, name=name, context=context
        )

    def set_task_factory(self, factory: Any) -> None:
        self._task_factory = factory

    def get_task_factory(self) -> Any:
        return self._task_factory

    # ------------------------------------------------------------------ lifecycle

    def is_running(self) -> bool:
        return self._running

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._ready.clear()
        self._closed = True

    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("event loop is closed")

    async def shutdown_asyncgens(self) -> None:
        return None

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        return None

    def run_forever(self) -> None:
        raise RuntimeError("the workflow loop is driven by the executor; use run_until_idle()")

    def run_until_complete(self, future: Any) -> Any:
        raise RuntimeError("the workflow loop is driven by the executor; use run_until_idle()")

    def stop(self) -> None:
        # Nothing to stop: run_until_idle() returns when no callback is ready.
        return None

    # ------------------------------------------------------------------ errors and debug

    def get_debug(self) -> bool:
        return self._debug

    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled

    def set_exception_handler(
        self,
        handler: Callable[[asyncio.AbstractEventLoop, dict[str, Any]], object] | None,
    ) -> None:
        self._exception_handler = handler

    def get_exception_handler(
        self,
    ) -> Callable[[asyncio.AbstractEventLoop, dict[str, Any]], object] | None:
        return self._exception_handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        self.unhandled_errors.append(context)
        exc = context.get("exception")
        logger.warning(
            "unhandled error in workflow code: %s",
            context.get("message", "unknown error"),
            exc_info=(type(exc), exc, exc.__traceback__)
            if isinstance(exc, BaseException)
            else None,
        )

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        if self._exception_handler is None:
            self.default_exception_handler(context)
            return
        try:
            self._exception_handler(self, context)
        except Exception:
            logger.exception("workflow exception handler failed")
            self.default_exception_handler(context)

    # ------------------------------------------------------------------ forbidden operations
    # Everything below would perform I/O or depend on the host. The permissive
    # ``*args, **kwargs`` signatures are compatible with every Python version's stubs.

    def run_in_executor(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        raise self._forbid("run_in_executor() / asyncio.to_thread()")

    def set_default_executor(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("set_default_executor()")

    def add_signal_handler(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        raise self._forbid("add_signal_handler()")

    def remove_signal_handler(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("remove_signal_handler()")

    def add_reader(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        raise self._forbid("add_reader()")

    def remove_reader(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("remove_reader()")

    def add_writer(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        raise self._forbid("add_writer()")

    def remove_writer(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("remove_writer()")

    async def getaddrinfo(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("getaddrinfo()")

    async def getnameinfo(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("getnameinfo()")

    async def create_connection(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("create_connection()")

    async def create_server(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("create_server()")

    async def create_datagram_endpoint(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("create_datagram_endpoint()")

    async def create_unix_connection(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("create_unix_connection()")

    async def create_unix_server(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("create_unix_server()")

    async def connect_accepted_socket(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("connect_accepted_socket()")

    async def connect_read_pipe(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("connect_read_pipe()")

    async def connect_write_pipe(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("connect_write_pipe()")

    async def subprocess_exec(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("subprocess_exec()")

    async def subprocess_shell(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("subprocess_shell()")

    async def sendfile(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sendfile()")

    async def sock_sendfile(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_sendfile()")

    async def start_tls(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("start_tls()")

    async def sock_recv(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_recv()")

    async def sock_recv_into(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_recv_into()")

    async def sock_recvfrom(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_recvfrom()")

    async def sock_recvfrom_into(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_recvfrom_into()")

    async def sock_sendall(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_sendall()")

    async def sock_sendto(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_sendto()")

    async def sock_connect(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_connect()")

    async def sock_accept(self, *args: Any, **kwargs: Any) -> Any:
        raise self._forbid("sock_accept()")
