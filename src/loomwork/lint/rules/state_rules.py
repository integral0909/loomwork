"""LW113: mutating module-global state from workflow code.

A module is imported once per worker process and stays imported: a live tick and a
replay of the same run can execute inside different processes (or the same process at
different times, with other runs interleaved), so a plain module global is not part of
the recorded history and is not shared between them. Assigning to one from a workflow
silently makes the workflow depend on - and corrupt - whatever process happens to run it.
"""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule

RULE_GLOBAL_MUTATION = DEFAULT_REGISTRY.register(
    Rule(
        code="LW113",
        name="global-mutation",
        severity=Severity.ERROR,
        message="assignment to global {name!r} mutates module state outside the history",
        hint="store the value in workflow-local state, or return it and let the caller "
        "decide what to do with it",
    )
)
