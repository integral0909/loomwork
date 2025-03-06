"""Human-readable reporter: ``file:line:col CODE message`` plus a fix hint."""

from __future__ import annotations

from loomwork.lint.models import LintResult


def render_text(result: LintResult) -> str:
    lines: list[str] = []
    for finding in result.findings:
        lines.append(f"{finding.location}: {finding.code} {finding.message}")
        lines.append(f"  hint: {finding.hint}")
        if finding.call_path:
            chain = " -> ".join((*finding.call_path, "here"))
            lines.append(f"  via: {chain}")
    if result.findings:
        lines.append("")
    lines.append(
        f"{len(result.findings)} finding(s) "
        f"({result.error_count} error(s), {result.warning_count} warning(s)) "
        f"in {result.files_scanned} file(s) scanned"
    )
    return "\n".join(lines) + "\n"
