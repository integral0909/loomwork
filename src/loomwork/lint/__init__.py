"""``loomwork.lint``: a static determinism analyzer for workflow functions.

Workflow functions are replayed by re-executing their code against recorded history (see
``docs/adr/0001-architecture.md``), so they must be deterministic. Runtime guard rails
(``loomwork.runtime.loop.DeterministicEventLoop``) catch violations when a workflow
actually runs; this package catches the same class of mistakes - and a few more that
have no runtime signal at all, like iterating a ``set`` - *before* a deploy, by walking
the AST of every ``@workflow``-decorated function.

.. code-block:: python

    from loomwork.lint import lint_paths

    result = lint_paths(["src/myapp"])
    for finding in result.findings:
        print(f"{finding.location}: {finding.code} {finding.message}")

See ``docs/lint-rules.md`` for the full rule catalog and ``python -m loomwork.lint
--help`` for the command line interface.
"""

from __future__ import annotations

# Importing the analyzer transitively imports loomwork.lint.rules, which registers every
# built-in rule into DEFAULT_REGISTRY as a side effect; config validation, the CLI and
# the SARIF reporter all depend on that having already happened by the time they run.
from loomwork.lint.analyzer import analyze_file, analyze_source, lint_paths, lint_source
from loomwork.lint.cli import main
from loomwork.lint.config import LintConfig
from loomwork.lint.errors import ConfigError, LintUsageError
from loomwork.lint.models import Finding, LintResult, Location, Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule, RuleRegistry

__all__ = [
    "DEFAULT_REGISTRY",
    "ConfigError",
    "Finding",
    "LintConfig",
    "LintResult",
    "LintUsageError",
    "Location",
    "Rule",
    "RuleRegistry",
    "Severity",
    "analyze_file",
    "analyze_source",
    "lint_paths",
    "lint_source",
    "main",
]
