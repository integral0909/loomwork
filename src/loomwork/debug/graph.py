"""Rendering a run's history as a timeline.

These are pure functions of a list of history events: no store, no workflow code, no
payload decoding, so they work on any history, including one still holding encrypted
payloads (only event *shapes* -- kinds, sequence numbers, timestamps -- are used, never
payload contents). :func:`build_timeline` extracts the underlying (step, timer, child,
wait) intervals once; the three renderers (:func:`render_mermaid`, :func:`render_dot`,
:func:`render_text`) format the same intervals for different audiences.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from loomwork.history.events import (
    CancelRequested,
    ChildCompleted,
    ChildFailed,
    ChildScheduled,
    Event,
    SignalReceived,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerCanceled,
    TimerFired,
    TimerStarted,
    UpdateRequested,
    WorkflowStarted,
)

#: Input events with no "started" counterpart: the gap since the previous tick ended is
#: a genuine wait (on a signal, an update or cancellation), not part of a step/timer/
#: child bar.
_WAIT_TRIGGERS = (SignalReceived, CancelRequested, UpdateRequested)


@dataclass(frozen=True, slots=True)
class TimelineEntry:
    """One bar of the timeline: a step, timer, child run or wait."""

    lane: str
    """``"step"``, ``"timer"``, ``"child"`` or ``"wait"``."""
    label: str
    start: datetime
    end: datetime | None
    """``None`` if the run's history ended before this entry resolved."""
    status: str
    seq: int | None = None


def _wait_label(event: Event) -> str:
    if isinstance(event, SignalReceived):
        return f"channel {event.channel!r}"
    if isinstance(event, CancelRequested):
        return "cancellation"
    if isinstance(event, UpdateRequested):
        return f"update {event.name!r}"
    return "wait"  # pragma: no cover - _WAIT_TRIGGERS covers every caller


def build_timeline(history: Sequence[Event]) -> list[TimelineEntry]:
    """Extract steps, timers, children and waits from ``history``, in start order."""
    entries: list[TimelineEntry] = []
    open_steps: dict[int, tuple[str, datetime]] = {}
    open_timers: dict[int, datetime] = {}
    open_children: dict[int, tuple[str, datetime]] = {}
    last_tick_end: datetime | None = None

    for event in history:
        if (
            isinstance(event, _WAIT_TRIGGERS)
            and last_tick_end is not None
            and event.timestamp > last_tick_end
        ):
            entries.append(
                TimelineEntry(
                    "wait", _wait_label(event), last_tick_end, event.timestamp, "resolved"
                )
            )
        match event:
            case WorkflowStarted():
                pass
            case StepScheduled(seq=seq, step_name=name):
                open_steps[seq] = (name, event.timestamp)
            case StepCompleted(seq=seq) | StepFailed(seq=seq):
                started = open_steps.pop(seq, None)
                if started is not None:
                    name, start = started
                    status = "completed" if isinstance(event, StepCompleted) else "failed"
                    entries.append(TimelineEntry("step", name, start, event.timestamp, status, seq))
            case TimerStarted(seq=seq):
                open_timers[seq] = event.timestamp
            case TimerFired(seq=seq) | TimerCanceled(seq=seq):
                if seq in open_timers:
                    start = open_timers.pop(seq)
                    status = "fired" if isinstance(event, TimerFired) else "canceled"
                    entries.append(
                        TimelineEntry("timer", f"timer {seq}", start, event.timestamp, status, seq)
                    )
            case ChildScheduled(seq=seq, workflow_name=name, workflow_id=workflow_id):
                open_children[seq] = (f"{name} ({workflow_id})", event.timestamp)
            case ChildCompleted(seq=seq) | ChildFailed(seq=seq):
                started = open_children.pop(seq, None)
                if started is not None:
                    label, start = started
                    status = "completed" if isinstance(event, ChildCompleted) else "failed"
                    entries.append(
                        TimelineEntry("child", label, start, event.timestamp, status, seq)
                    )
            case TickCompleted():
                last_tick_end = event.timestamp
            case _:
                pass

    for seq, (name, start) in open_steps.items():
        entries.append(TimelineEntry("step", name, start, None, "pending", seq))
    for seq, start in open_timers.items():
        entries.append(TimelineEntry("timer", f"timer {seq}", start, None, "pending", seq))
    for seq, (label, start) in open_children.items():
        entries.append(TimelineEntry("child", label, start, None, "pending", seq))

    entries.sort(key=lambda e: (e.start, e.lane, e.seq or 0))
    return entries


_LANE_TITLES = {"step": "Steps", "timer": "Timers", "child": "Children", "wait": "Waits"}
_MERMAID_STATUS = {
    "completed": "done",
    "fired": "done",
    "resolved": "done",
    "failed": "crit",
    "canceled": "crit",
    "pending": "active",
}


def _duration_seconds(entry: TimelineEntry, history_end: datetime) -> int:
    end = entry.end if entry.end is not None else history_end
    return max(1, round((end - entry.start).total_seconds()))


def render_mermaid(history: Sequence[Event], *, title: str = "") -> str:
    """A Mermaid ``gantt`` diagram with one section per lane (steps, timers, ...)."""
    entries = build_timeline(history)
    lines = ["gantt", f"    title {title}" if title else "    title workflow run"]
    lines.append("    dateFormat  YYYY-MM-DDTHH:mm:ss")
    if not entries:
        lines.append("    section Timeline")
        lines.append("    (no steps, timers, children or waits recorded) : done, empty, 0, 1s")
        return "\n".join(lines)
    history_end = max(e.end or e.start for e in entries)
    counters: dict[str, int] = {}
    for lane in ("step", "timer", "child", "wait"):
        lane_entries = [e for e in entries if e.lane == lane]
        if not lane_entries:
            continue
        lines.append(f"    section {_LANE_TITLES[lane]}")
        for entry in lane_entries:
            counters[lane] = counters.get(lane, 0) + 1
            task_id = f"{lane}_{counters[lane]}"
            status = _MERMAID_STATUS.get(entry.status, "active")
            start = entry.start.strftime("%Y-%m-%dT%H:%M:%S")
            duration = _duration_seconds(entry, history_end)
            label = entry.label.replace(":", "-")
            lines.append(
                f"    {label} ({entry.status}) : {status}, {task_id}, {start}, {duration}s"
            )
    return "\n".join(lines)


_DOT_COLORS = {
    "completed": "palegreen3",
    "fired": "palegreen3",
    "resolved": "palegreen3",
    "failed": "firebrick2",
    "canceled": "gray70",
    "pending": "gold2",
}


def _dot_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


def render_dot(history: Sequence[Event], *, title: str = "") -> str:
    """A Graphviz ``digraph``: one cluster per lane, nodes in start order within it."""
    entries = build_timeline(history)
    lines = ["digraph history {", '    rankdir="LR";', "    node [shape=box, style=filled];"]
    if title:
        lines.append(f'    label="{_dot_escape(title)}"; labelloc=t;')
    for lane in ("step", "timer", "child", "wait"):
        lane_entries = [e for e in entries if e.lane == lane]
        if not lane_entries:
            continue
        lines.append(f'    subgraph cluster_{lane} {{ label="{_LANE_TITLES[lane]}";')
        previous: str | None = None
        for index, entry in enumerate(lane_entries):
            node_id = f"{lane}_{index}"
            color = _DOT_COLORS.get(entry.status, "lightgray")
            end_text = entry.end.isoformat() if entry.end is not None else "open"
            # Escape each untrusted piece on its own, then join with a literal DOT
            # line-break directive -- escaping the joined string would double it too,
            # turning the line break back into visible "\n" text.
            label = "\\n".join(
                _dot_escape(part)
                for part in (
                    entry.label,
                    f"{entry.start.isoformat()} -> {end_text}",
                    entry.status,
                )
            )
            lines.append(f'        {node_id} [label="{label}", fillcolor={color}];')
            if previous is not None:
                lines.append(f"        {previous} -> {node_id};")
            previous = node_id
        lines.append("    }")
    lines.append("}")
    return "\n".join(lines)


def render_text(history: Sequence[Event], *, title: str = "") -> str:
    """A compact, terminal-friendly timeline, one line per entry."""
    entries = build_timeline(history)
    lines = [title] if title else []
    if not entries:
        lines.append("(no steps, timers, children or waits recorded)")
        return "\n".join(lines)
    width = max(len(e.lane) for e in entries)
    for entry in entries:
        start = entry.start.isoformat()
        end = entry.end.isoformat() if entry.end is not None else "..."
        lines.append(
            f"{start}  {entry.lane.ljust(width)}  {entry.label:<24}  {entry.status:<10}  -> {end}"
        )
    return "\n".join(lines)
