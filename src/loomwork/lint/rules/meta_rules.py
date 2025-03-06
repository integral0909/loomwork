"""Rules that are not about a specific determinism concern.

``LW000`` reports a file the linter could not even parse (so every other rule silently
found nothing to check, which would otherwise look like a clean file). ``LW199`` reports
a project-specific forbidden call added through ``extend-forbidden-calls`` in
``pyproject.toml``; see :mod:`loomwork.lint.config`.
"""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule

RULE_PARSE_ERROR = DEFAULT_REGISTRY.register(
    Rule(
        code="LW000",
        name="parse-error",
        severity=Severity.ERROR,
        message="could not parse this file: {name}",
        hint="fix the syntax error; no other rule could be checked in this file",
    )
)

RULE_CUSTOM_FORBIDDEN_CALL = DEFAULT_REGISTRY.register(
    Rule(
        code="LW199",
        name="custom-forbidden-call",
        severity=Severity.ERROR,
        message="{name}() is forbidden in workflow code by project configuration",
        hint="move this call into a step, or remove it from extend-forbidden-calls if it is "
        "actually deterministic",
    )
)
