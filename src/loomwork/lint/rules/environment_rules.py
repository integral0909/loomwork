"""LW110: environment variables.

Unlike most rules this one is a warning, not an error: an environment variable that is
fixed for the lifetime of the worker process (a feature flag set at deploy time, say) is
usually stable enough in practice, but it is still a form of ambient, unjournaled input
that *can* change between the original run and a replay on a different worker.
"""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule
from loomwork.lint.rules.tables import ForbiddenCall, MatchKind

RULE_ENVIRONMENT_ACCESS = DEFAULT_REGISTRY.register(
    Rule(
        code="LW110",
        name="environment-access",
        severity=Severity.WARNING,
        message="{name} reads an environment variable, whose value may differ on replay",
        hint="read it once at process startup and pass it in, or move the read into a step",
    )
)

FORBIDDEN_CALLS = (
    ForbiddenCall("os.environ", RULE_ENVIRONMENT_ACCESS.code, MatchKind.PREFIX),
    ForbiddenCall("os.getenv", RULE_ENVIRONMENT_ACCESS.code),
)
