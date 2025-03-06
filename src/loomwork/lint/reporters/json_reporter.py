"""Machine-readable JSON reporter."""

from __future__ import annotations

import json
from typing import Any

from loomwork.lint.models import Finding, LintResult

#: Bumped if the shape of the JSON output changes incompatibly.
SCHEMA_VERSION = 1


def render_json(result: LintResult) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "files_scanned": result.files_scanned,
        "error_count": result.error_count,
        "warning_count": result.warning_count,
        "findings": [_finding_to_dict(finding) for finding in result.findings],
    }
    return json.dumps(payload, indent=2, sort_keys=False) + "\n"


def _finding_to_dict(finding: Finding) -> dict[str, Any]:
    return {
        "code": finding.code,
        "name": finding.name,
        "severity": finding.severity.value,
        "message": finding.message,
        "hint": finding.hint,
        "location": {
            "file": finding.location.file,
            "line": finding.location.line,
            "column": finding.location.column,
            "end_line": finding.location.end_line,
            "end_column": finding.location.end_column,
        },
        "call_path": list(finding.call_path),
    }
