"""Commands produced by workflow code.

A command is the workflow's *request*; the engine accepts it by recording the matching
decision event and performing the side effect. Each command knows the decision event it
becomes (:meth:`Command.to_event`) and how to recognise that event during replay
(:meth:`Command.matches`).

Matching is deliberately narrow: it compares the command kind, its sequence number and
the *identity* of the operation (for example the step name), but not arguments or
durations. Changing a step's input or a sleep's length between deploys is a legitimate
code change and must not break running workflows; calling a different step at the same
point is not.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    Event,
    StepCancelRequested,
    StepScheduled,
    TimerCanceled,
    TimerStarted,
    WorkflowCanceled,
    WorkflowCompleted,
    WorkflowFailed,
)
from loomwork.options import StepOptions


class Command:
    """Base class for workflow commands."""

    __slots__ = ()

    def to_event(self) -> Event:
        raise NotImplementedError

    def matches(self, event: Event) -> bool:
        raise NotImplementedError

    def describe(self) -> str:
        """Short description used in nondeterminism error messages."""
        return type(self).__name__

    @property
    def closes_workflow(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class ScheduleStep(Command):
    seq: int
    step_name: str
    args: list[Payload] = field(default_factory=list)
    options: StepOptions = field(default_factory=StepOptions)

    def to_event(self) -> Event:
        return StepScheduled(
            seq=self.seq, step_name=self.step_name, args=self.args, options=self.options
        )

    def matches(self, event: Event) -> bool:
        return (
            isinstance(event, StepScheduled)
            and event.seq == self.seq
            and event.step_name == self.step_name
        )

    def describe(self) -> str:
        return f"ScheduleStep(seq={self.seq}, step={self.step_name!r})"


@dataclass(frozen=True, slots=True)
class RequestCancelStep(Command):
    seq: int

    def to_event(self) -> Event:
        return StepCancelRequested(seq=self.seq)

    def matches(self, event: Event) -> bool:
        return isinstance(event, StepCancelRequested) and event.seq == self.seq

    def describe(self) -> str:
        return f"RequestCancelStep(seq={self.seq})"


@dataclass(frozen=True, slots=True)
class StartTimer(Command):
    seq: int
    fire_at: datetime

    def to_event(self) -> Event:
        return TimerStarted(seq=self.seq, fire_at=self.fire_at)

    def matches(self, event: Event) -> bool:
        return isinstance(event, TimerStarted) and event.seq == self.seq

    def describe(self) -> str:
        return f"StartTimer(seq={self.seq}, fire_at={self.fire_at.isoformat()})"


@dataclass(frozen=True, slots=True)
class CancelTimer(Command):
    seq: int

    def to_event(self) -> Event:
        return TimerCanceled(seq=self.seq)

    def matches(self, event: Event) -> bool:
        return isinstance(event, TimerCanceled) and event.seq == self.seq

    def describe(self) -> str:
        return f"CancelTimer(seq={self.seq})"


@dataclass(frozen=True, slots=True)
class CompleteWorkflow(Command):
    result: Payload

    def to_event(self) -> Event:
        return WorkflowCompleted(result=self.result)

    def matches(self, event: Event) -> bool:
        return isinstance(event, WorkflowCompleted)

    @property
    def closes_workflow(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class FailWorkflow(Command):
    failure: FailureInfo

    def to_event(self) -> Event:
        return WorkflowFailed(failure=self.failure)

    def matches(self, event: Event) -> bool:
        return isinstance(event, WorkflowFailed)

    def describe(self) -> str:
        return f"FailWorkflow(type={self.failure.type!r})"

    @property
    def closes_workflow(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class CancelWorkflow(Command):
    reason: str = ""

    def to_event(self) -> Event:
        return WorkflowCanceled(reason=self.reason)

    def matches(self, event: Event) -> bool:
        return isinstance(event, WorkflowCanceled)

    @property
    def closes_workflow(self) -> bool:
        return True


def describe_event(event: Event) -> str:
    """Short description of a decision event, mirroring :meth:`Command.describe`."""
    seq = getattr(event, "seq", None)
    name = getattr(event, "step_name", None)
    parts = []
    if seq is not None:
        parts.append(f"seq={seq}")
    if name is not None:
        parts.append(f"step={name!r}")
    return f"{type(event).__name__}({', '.join(parts)})"
