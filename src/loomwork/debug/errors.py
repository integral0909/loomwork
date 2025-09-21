"""Errors raised while tracing a replay."""

from __future__ import annotations


class DebugError(Exception):
    """The history or workflow code could not be replayed for debugging.

    Unlike :class:`~loomwork.errors.NondeterminismError`, this is never raised for a
    mismatch between the code and the recorded history -- that is reported as a
    :class:`~loomwork.debug.tracer.Divergence` on the trace instead, so a caller can
    show everything replayed correctly up to that point.
    """
