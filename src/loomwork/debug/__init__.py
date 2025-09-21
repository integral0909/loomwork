"""Replay debugging and history visualization.

:func:`~loomwork.debug.tracer.trace_replay` replays a recorded history tick by tick
against workflow code, producing a structured trace instead of raising on the first
nondeterminism (see :mod:`loomwork.debug.tracer`). :mod:`loomwork.debug.graph` renders a
history's steps, timers, children and waits as Mermaid, Graphviz or plain text.
"""

from __future__ import annotations

from loomwork.debug.errors import DebugError
from loomwork.debug.graph import (
    TimelineEntry,
    build_timeline,
    render_dot,
    render_mermaid,
    render_text,
)
from loomwork.debug.tracer import Divergence, ReplayTrace, TickTrace, trace_replay

__all__ = [
    "DebugError",
    "Divergence",
    "ReplayTrace",
    "TickTrace",
    "TimelineEntry",
    "build_timeline",
    "render_dot",
    "render_mermaid",
    "render_text",
    "trace_replay",
]
