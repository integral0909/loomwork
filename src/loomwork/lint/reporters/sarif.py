"""SARIF 2.1.0 reporter, for GitHub code scanning and other SARIF consumers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loomwork._version import __version__
from loomwork.lint.models import Finding, LintResult, Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, RuleRegistry

SARIF_VERSION = "2.1.0"
SARIF_SCHEMA = (
    "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/"
    "sarif-schema-2.1.0.json"
)
_DOCS_URL = "https://github.com/integral0909/loomwork/blob/main/docs/lint-rules.md"


def render_sarif(result: LintResult, *, registry: RuleRegistry = DEFAULT_REGISTRY) -> str:
    document = {
        "$schema": SARIF_SCHEMA,
        "version": SARIF_VERSION,
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "loomwork-lint",
                        "version": __version__,
                        "informationUri": "https://github.com/integral0909/loomwork",
                        "rules": _rule_descriptors(result, registry),
                    }
                },
                "results": [_result_entry(finding) for finding in result.findings],
            }
        ],
    }
    return json.dumps(document, indent=2) + "\n"


def _rule_descriptors(result: LintResult, registry: RuleRegistry) -> list[dict[str, Any]]:
    codes = sorted({finding.code for finding in result.findings})
    descriptors: list[dict[str, Any]] = []
    for code in codes:
        rule = registry.get(code)
        if rule is None:
            continue
        descriptors.append(
            {
                "id": rule.code,
                "name": rule.name,
                "shortDescription": {"text": rule.name.replace("-", " ")},
                "fullDescription": {"text": rule.hint},
                "helpUri": f"{_DOCS_URL}#{rule.code.lower()}",
                "defaultConfiguration": {
                    "level": "error" if rule.severity is Severity.ERROR else "warning"
                },
                "properties": {"tags": ["determinism", "loomwork"]},
            }
        )
    return descriptors


def _result_entry(finding: Finding) -> dict[str, Any]:
    message = finding.message
    if finding.call_path:
        message = f"{message} (reached via {' -> '.join(finding.call_path)})"
    region: dict[str, int] = {
        "startLine": finding.location.line,
        "startColumn": finding.location.column,
    }
    if finding.location.end_line is not None:
        region["endLine"] = finding.location.end_line
    if finding.location.end_column is not None:
        region["endColumn"] = finding.location.end_column
    return {
        "ruleId": finding.code,
        "level": "error" if finding.severity is Severity.ERROR else "warning",
        "message": {"text": message},
        "locations": [
            {
                "physicalLocation": {
                    "artifactLocation": {"uri": Path(finding.location.file).as_posix()},
                    "region": region,
                }
            }
        ],
    }
