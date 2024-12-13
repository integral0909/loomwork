"""History event types.

Events fall into four categories (see ADR-0002):

``INPUT``
    Something that happened *to* the workflow and must be delivered to its code:
    the start itself, step outcomes, timer firings, cancellation requests.
``MARKER``
    Structural events. :class:`TickCompleted` delimits activations.
``DECISION``
    Recorded outcomes of workflow commands. During replay they are matched against the
    commands the code produces.
``SYSTEM``
    Events the engine appends on its own authority, such as termination. They close the
    run without the workflow code taking part.

Events are immutable. ``event_id`` and ``timestamp`` are assigned by the store when the
event is appended; constructors leave them at their defaults.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.options import StepOptions

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

#: Bumped whenever an incompatible change is made to the event model.
HISTORY_FORMAT_VERSION = 1


class EventCategory(enum.StrEnum):
    INPUT = "input"
    MARKER = "marker"
    DECISION = "decision"
    SYSTEM = "system"


EVENT_TYPES: dict[str, type[Event]] = {}


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    """Base class of all history events."""

    kind: ClassVar[str] = ""
    category: ClassVar[EventCategory]

    event_id: int = 0
    timestamp: datetime = EPOCH

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # No super() call: dataclass(slots=True) rebuilds this class, which breaks the
        # zero-argument form, and object.__init_subclass__ does nothing.
        if cls.kind:
            existing = EVENT_TYPES.get(cls.kind)
            # dataclass(slots=True) re-creates the class, so the same definition is
            # seen twice; the second (slotted) class replaces the first.
            if existing is not None and (
                existing.__qualname__ != cls.__qualname__ or existing.__module__ != cls.__module__
            ):
                raise TypeError(f"duplicate event kind {cls.kind!r}")
            EVENT_TYPES[cls.kind] = cls

    @property
    def is_input(self) -> bool:
        return self.category is EventCategory.INPUT

    @property
    def is_decision(self) -> bool:
        return self.category is EventCategory.DECISION


# --------------------------------------------------------------------------- inputs


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowStarted(Event):
    kind: ClassVar[str] = "workflow_started"
    category: ClassVar[EventCategory] = EventCategory.INPUT

    workflow_name: str
    workflow_id: str
    run_id: str
    args: list[Payload] = field(default_factory=list)
    random_seed: int = 0
    attempt: int = 1


@dataclass(frozen=True, slots=True, kw_only=True)
class StepCompleted(Event):
    kind: ClassVar[str] = "step_completed"
    category: ClassVar[EventCategory] = EventCategory.INPUT

    seq: int
    result: Payload
    attempts: int = 1


@dataclass(frozen=True, slots=True, kw_only=True)
class StepFailed(Event):
    kind: ClassVar[str] = "step_failed"
    category: ClassVar[EventCategory] = EventCategory.INPUT

    seq: int
    failure: FailureInfo
    attempts: int = 1


@dataclass(frozen=True, slots=True, kw_only=True)
class TimerFired(Event):
    kind: ClassVar[str] = "timer_fired"
    category: ClassVar[EventCategory] = EventCategory.INPUT

    seq: int


@dataclass(frozen=True, slots=True, kw_only=True)
class CancelRequested(Event):
    kind: ClassVar[str] = "cancel_requested"
    category: ClassVar[EventCategory] = EventCategory.INPUT

    reason: str = ""


# --------------------------------------------------------------------------- markers


@dataclass(frozen=True, slots=True, kw_only=True)
class TickCompleted(Event):
    kind: ClassVar[str] = "tick_completed"
    category: ClassVar[EventCategory] = EventCategory.MARKER

    consumed_through: int
    time: datetime
    worker_id: str = ""


# --------------------------------------------------------------------------- decisions


@dataclass(frozen=True, slots=True, kw_only=True)
class StepScheduled(Event):
    kind: ClassVar[str] = "step_scheduled"
    category: ClassVar[EventCategory] = EventCategory.DECISION

    seq: int
    step_name: str
    args: list[Payload] = field(default_factory=list)
    options: StepOptions = field(default_factory=StepOptions)


@dataclass(frozen=True, slots=True, kw_only=True)
class StepCancelRequested(Event):
    kind: ClassVar[str] = "step_cancel_requested"
    category: ClassVar[EventCategory] = EventCategory.DECISION

    seq: int


@dataclass(frozen=True, slots=True, kw_only=True)
class TimerStarted(Event):
    kind: ClassVar[str] = "timer_started"
    category: ClassVar[EventCategory] = EventCategory.DECISION

    seq: int
    fire_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class TimerCanceled(Event):
    kind: ClassVar[str] = "timer_canceled"
    category: ClassVar[EventCategory] = EventCategory.DECISION

    seq: int


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowCompleted(Event):
    kind: ClassVar[str] = "workflow_completed"
    category: ClassVar[EventCategory] = EventCategory.DECISION

    result: Payload


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowFailed(Event):
    kind: ClassVar[str] = "workflow_failed"
    category: ClassVar[EventCategory] = EventCategory.DECISION

    failure: FailureInfo


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowCanceled(Event):
    kind: ClassVar[str] = "workflow_canceled"
    category: ClassVar[EventCategory] = EventCategory.DECISION

    reason: str = ""


# --------------------------------------------------------------------------- system


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkflowTerminated(Event):
    kind: ClassVar[str] = "workflow_terminated"
    category: ClassVar[EventCategory] = EventCategory.SYSTEM

    reason: str = ""


CLOSING_EVENTS: tuple[type[Event], ...] = (
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowCanceled,
    WorkflowTerminated,
)


def is_closing(event: Event) -> bool:
    return isinstance(event, CLOSING_EVENTS)
