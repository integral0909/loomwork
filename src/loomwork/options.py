"""Execution options shared by the SDK, the history model and the engine."""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from loomwork.retry import RetryPolicy

DEFAULT_QUEUE = "default"
_QUEUE_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,100}$")
MIN_PRIORITY = -1000
MAX_PRIORITY = 1000


class ParentClosePolicy(enum.StrEnum):
    """What happens to a running child workflow when its parent closes first."""

    TERMINATE = "terminate"
    REQUEST_CANCEL = "request_cancel"
    ABANDON = "abandon"


def as_timedelta(value: timedelta | float | None) -> timedelta | None:
    """Interpret numbers as seconds; pass ``timedelta`` and ``None`` through."""
    if value is None or isinstance(value, timedelta):
        return value
    return timedelta(seconds=value)


def validate_queue(name: str) -> str:
    if not _QUEUE_RE.match(name):
        raise ValueError(f"invalid queue name {name!r}: use 1-100 letters, digits and _ . : -")
    return name


@dataclass(frozen=True, slots=True, init=False)
class StepOptions:
    """How a step is executed.

    Every field is optional; unset fields fall back to the engine configuration.

    Attributes:
        retry: retry policy.
        timeout: maximum duration of a single attempt.
        total_timeout: maximum duration from scheduling until the final outcome,
            across all attempts and backoff delays.
        queue: the task queue the step is dispatched to. Only workers subscribed to
            the queue execute it, which is how steps are routed to specialised
            machines (GPU hosts, workers holding payment credentials, ...).
        priority: higher values are claimed first within a queue
            (``-1000..1000``, default ``0``).
        heartbeat_timeout: if set, an attempt must call :func:`loomwork.heartbeat` at
            least this often; a silent attempt is considered lost and retried long
            before its ``timeout`` expires.
    """

    retry: RetryPolicy | None
    timeout: timedelta | None
    total_timeout: timedelta | None
    queue: str | None
    priority: int | None
    heartbeat_timeout: timedelta | None

    def __init__(
        self,
        retry: RetryPolicy | None = None,
        timeout: timedelta | float | None = None,
        total_timeout: timedelta | float | None = None,
        queue: str | None = None,
        priority: int | None = None,
        heartbeat_timeout: timedelta | float | None = None,
    ) -> None:
        object.__setattr__(self, "retry", retry)
        object.__setattr__(self, "timeout", as_timedelta(timeout))
        object.__setattr__(self, "total_timeout", as_timedelta(total_timeout))
        object.__setattr__(self, "queue", validate_queue(queue) if queue is not None else None)
        object.__setattr__(self, "priority", priority)
        object.__setattr__(self, "heartbeat_timeout", as_timedelta(heartbeat_timeout))
        for name in ("timeout", "total_timeout", "heartbeat_timeout"):
            value = getattr(self, name)
            if value is not None and value <= timedelta(0):
                raise ValueError(f"{name} must be positive")
        if priority is not None and not MIN_PRIORITY <= priority <= MAX_PRIORITY:
            raise ValueError(f"priority must be between {MIN_PRIORITY} and {MAX_PRIORITY}")

    def merged(self, override: StepOptions) -> StepOptions:
        """Options where every field set on ``override`` wins."""

        def pick(name: str) -> Any:
            value = getattr(override, name)
            return value if value is not None else getattr(self, name)

        return StepOptions(
            retry=pick("retry"),
            timeout=pick("timeout"),
            total_timeout=pick("total_timeout"),
            queue=pick("queue"),
            priority=pick("priority"),
            heartbeat_timeout=pick("heartbeat_timeout"),
        )

    @property
    def effective_queue(self) -> str:
        return self.queue or DEFAULT_QUEUE

    @property
    def effective_priority(self) -> int:
        return self.priority or 0

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.retry is not None:
            data["retry"] = self.retry.to_dict()
        if self.timeout is not None:
            data["timeout"] = self.timeout.total_seconds()
        if self.total_timeout is not None:
            data["total_timeout"] = self.total_timeout.total_seconds()
        if self.queue is not None:
            data["queue"] = self.queue
        if self.priority is not None:
            data["priority"] = self.priority
        if self.heartbeat_timeout is not None:
            data["heartbeat_timeout"] = self.heartbeat_timeout.total_seconds()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StepOptions:
        retry = data.get("retry")
        priority = data.get("priority")
        return cls(
            retry=RetryPolicy.from_dict(retry) if retry is not None else None,
            timeout=as_timedelta(data.get("timeout")),
            total_timeout=as_timedelta(data.get("total_timeout")),
            queue=data.get("queue"),
            priority=int(priority) if priority is not None else None,
            heartbeat_timeout=as_timedelta(data.get("heartbeat_timeout")),
        )
