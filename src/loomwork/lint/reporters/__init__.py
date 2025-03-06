"""Renderers that turn a :class:`~loomwork.lint.models.LintResult` into text.

Every reporter is a pure function of the result (plus, for SARIF, the rule registry used
to describe each code) and produces byte-for-byte the same output for the same input:
findings are always consumed in the order the analyzer already sorted them in, so CI logs
and diffs stay stable across runs.
"""

from __future__ import annotations

from collections.abc import Callable

from loomwork.lint.models import LintResult
from loomwork.lint.reporters.json_reporter import render_json
from loomwork.lint.reporters.sarif import render_sarif
from loomwork.lint.reporters.text import render_text

#: Reporter functions by the ``--format`` name used on the command line.
REPORTERS: dict[str, Callable[[LintResult], str]] = {
    "text": render_text,
    "json": render_json,
    "sarif": render_sarif,
}

__all__ = ["REPORTERS", "render_json", "render_sarif", "render_text"]
