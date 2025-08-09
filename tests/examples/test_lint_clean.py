"""Every example's workflow code must pass the determinism linter cleanly."""

from __future__ import annotations

from pathlib import Path

from loomwork.lint import lint_paths

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


def test_examples_have_no_lint_findings() -> None:
    result = lint_paths([EXAMPLES_DIR])

    assert result.files_scanned > 0
    if result.has_findings:
        details = "\n".join(
            f"{finding.location}: {finding.code} {finding.message}" for finding in result.findings
        )
        raise AssertionError(f"expected no lint findings under examples/, got:\n{details}")
