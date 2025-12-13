"""Execution options shared by the SDK, the history model and the engine."""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

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


class IdReusePolicy(enum.StrEnum):
    """What starting a workflow id may do when its *latest* run has already closed.

    Evaluated only when there is no currently running execution of the id; see
    :class:`IdConflictPolicy` for that case.
    """

    ALLOW_DUPLICATE = "allow_duplicate"
    """Always start a new run. The default, and the historical behaviour."""

    ALLOW_DUPLICATE_FAILED_ONLY = "allow_duplicate_failed_only"
    """Start a new run only if the latest run did not complete successfully (it failed,
    was cancelled, was terminated, or timed out). Raises
    :class:`~loomwork.errors.WorkflowIdReuseRejectedError` if the latest run completed."""

    REJECT_DUPLICATE = "reject_duplicate"
    """Never start a new run while any run (open or closed) has used this id. Raises
    :class:`~loomwork.errors.WorkflowIdReuseRejectedError`."""


class VersioningPolicy(enum.StrEnum):
    """How a run's ticks are routed to workers declaring a build id (see
    ``docs/adr/0006-worker-build-versioning.md``).

    Orthogonal to :func:`loomwork.patched`: a policy decides *which worker* may run a
    tick, while a patch decides what the *same* worker's code does differently for old
    and new histories. Ignored entirely when no worker in the deployment sets
    :attr:`~loomwork.worker.worker.WorkerOptions.build_id` -- with no build ids in play,
    every run behaves exactly as before this feature existed.
    """

    AUTO_UPGRADE = "auto_upgrade"
    """Any worker may process any tick of the run, on whatever build happens to claim
    it (today's behaviour, and the default). Safe only for changes ``patched()`` (or a
    compatibility check) already establishes are safe for every in-flight run."""

    PINNED = "pinned"
    """The run's ticks are handed only to a worker whose build matches the build that
    ran its first tick, or one declared compatible with it. Lets incompatible code ship
    side by side with the build still draining old runs, with no patch required."""


class IdConflictPolicy(enum.StrEnum):
    """What starting a workflow id should do when it is *currently running*."""

    FAIL = "fail"
    """Raise :class:`~loomwork.errors.WorkflowAlreadyStartedError`. The default, and the
    historical behaviour."""

    USE_EXISTING = "use_existing"
    """Return a handle to the already-running execution instead of starting a new one."""

    TERMINATE_EXISTING = "terminate_existing"
    """Terminate the running execution and start the new run in its place, atomically:
    no worker ever observes both runs active for the same workflow id."""


@dataclass(frozen=True, slots=True)
class ResetPoint:
    """Where to truncate a run's history when resetting it (see :meth:`Engine.reset`).

    A reset point always names a **tick boundary**: a freeze-frame of the run's history
    exactly as it stood the moment some tick committed (its ``TickCompleted`` plus the
    decision events it produced), with everything from that point on dropped -- including
    any input that arrived before the *next* tick ran but that this tick itself never
    consumed. Construct one with a factory method rather than the constructor.
    """

    _kind: Literal["first_tick", "after_tick", "before_last_failure"]
    _tick: int | None = None

    @classmethod
    def first_tick(cls) -> ResetPoint:
        """Keep only ``WorkflowStarted``: the new run replays from scratch."""
        return cls("first_tick")

    @classmethod
    def after_tick(cls, count: int) -> ResetPoint:
        """Keep the first ``count`` completed ticks (and drop everything after).

        ``count=0`` is equivalent to :meth:`first_tick`.
        """
        if count < 0:
            raise ValueError("count must be >= 0")
        return cls("after_tick", count)

    @classmethod
    def before_last_failure(cls) -> ResetPoint:
        """Keep every completed tick except the run's last one.

        Since a tick that raises :class:`~loomwork.errors.NondeterminismError` is never
        committed to history (ADR-0002), the tick that produced a run's final, recorded
        outcome (a business failure, a cancellation, a termination, ...) is always its
        *last* committed tick. Dropping that tick reverts the decisions it made (for
        example, the step call whose result triggered a bad code path) while keeping
        everything that came before. A run with zero or one completed ticks resets to
        :meth:`first_tick`.
        """
        return cls("before_last_failure")

    @property
    def kind(self) -> Literal["first_tick", "after_tick", "before_last_failure"]:
        return self._kind

    @property
    def tick_count(self) -> int | None:
        """The ``count`` given to :meth:`after_tick`; ``None`` for other kinds."""
        return self._tick


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
