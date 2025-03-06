"""Exceptions raised by the linter itself (as opposed to findings it reports).

These signal misuse of the linter (a bad config value, an unknown rule code, a path that
does not exist) rather than a problem in the code being linted; :func:`loomwork.lint.main`
maps them to the "usage error" exit code (2) instead of the "findings" one (1).
"""

from __future__ import annotations


class LintUsageError(Exception):
    """The linter itself was misused: bad CLI arguments, config, or paths."""


class ConfigError(LintUsageError):
    """``[tool.loomwork.lint]`` in ``pyproject.toml`` (or an explicit config) is invalid."""
