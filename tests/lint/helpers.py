"""Shared helpers for the linter test suite."""

from __future__ import annotations

import textwrap

from loomwork.lint.analyzer import analyze_source
from loomwork.lint.config import LintConfig
from loomwork.lint.models import Finding


def lint(source: str, config: LintConfig | None = None, filename: str = "wf.py") -> list[Finding]:
    """Dedent and lint a source snippet, returning its findings."""
    return analyze_source(textwrap.dedent(source), filename, config)


def codes_of(findings: list[Finding]) -> list[str]:
    return [f.code for f in findings]
