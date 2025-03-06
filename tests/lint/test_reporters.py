"""The text, JSON and SARIF reporters."""

from __future__ import annotations

import json

from loomwork.lint.models import Finding, LintResult, Location, Severity
from loomwork.lint.reporters import REPORTERS, render_json, render_sarif, render_text
from tests.lint.helpers import lint


def _sample_result() -> list[Finding]:
    return lint(
        """
        import time
        import loomwork as lw

        def helper() -> float:
            return time.time()

        @lw.workflow
        async def process() -> None:
            helper()
            print("hi")
        """
    )


class TestTextReporter:
    def test_renders_location_code_message_and_hint(self) -> None:
        findings = _sample_result()
        result = LintResult(findings=tuple(findings), files_scanned=1)
        text = render_text(result)
        assert "LW101" in text
        assert "LW104" in text
        assert "hint:" in text
        assert "wf.py:" in text
        assert "2 finding(s)" in text

    def test_shows_the_call_path_for_transitive_findings(self) -> None:
        findings = _sample_result()
        result = LintResult(findings=tuple(findings), files_scanned=1)
        text = render_text(result)
        assert "via: process -> here" in text

    def test_clean_result_has_no_findings_section(self) -> None:
        result = LintResult(findings=(), files_scanned=3)
        text = render_text(result)
        assert "0 finding(s)" in text
        assert "hint:" not in text


class TestJsonReporter:
    def test_produces_valid_json_with_expected_shape(self) -> None:
        findings = _sample_result()
        result = LintResult(findings=tuple(findings), files_scanned=1)
        payload = json.loads(render_json(result))
        assert payload["files_scanned"] == 1
        assert payload["error_count"] == 1
        assert payload["warning_count"] == 1
        codes = {f["code"] for f in payload["findings"]}
        assert codes == {"LW101", "LW104"}
        lw101 = next(f for f in payload["findings"] if f["code"] == "LW101")
        assert lw101["call_path"] == ["process"]
        assert lw101["location"]["file"] == "wf.py"
        assert lw101["severity"] == "error"

    def test_is_deterministic_across_renders(self) -> None:
        findings = _sample_result()
        result = LintResult(findings=tuple(findings), files_scanned=1)
        assert render_json(result) == render_json(result)


class TestSarifReporter:
    def test_has_the_expected_sarif_2_1_0_shape(self) -> None:
        findings = _sample_result()
        result = LintResult(findings=tuple(findings), files_scanned=1)
        document = json.loads(render_sarif(result))
        assert document["version"] == "2.1.0"
        assert "$schema" in document
        run = document["runs"][0]
        assert run["tool"]["driver"]["name"] == "loomwork-lint"
        rule_ids = {rule["id"] for rule in run["tool"]["driver"]["rules"]}
        assert rule_ids == {"LW101", "LW104"}
        assert len(run["results"]) == 2
        for entry in run["results"]:
            assert entry["level"] in {"error", "warning"}
            location = entry["locations"][0]["physicalLocation"]
            assert location["artifactLocation"]["uri"] == "wf.py"
            assert location["region"]["startLine"] > 0
            assert location["region"]["startColumn"] > 0

    def test_call_path_is_included_in_the_message(self) -> None:
        findings = _sample_result()
        result = LintResult(findings=tuple(findings), files_scanned=1)
        document = json.loads(render_sarif(result))
        lw101 = next(r for r in document["runs"][0]["results"] if r["ruleId"] == "LW101")
        assert "process" in lw101["message"]["text"]

    def test_empty_result_still_produces_a_valid_document(self) -> None:
        document = json.loads(render_sarif(LintResult(findings=(), files_scanned=0)))
        assert document["runs"][0]["results"] == []
        assert document["runs"][0]["tool"]["driver"]["rules"] == []

    def test_a_finding_with_an_unregistered_code_is_still_reported_without_a_descriptor(
        self,
    ) -> None:
        # Defensive: a finding referencing a code the given registry does not know about
        # (a custom registry in some embedding, say) still produces a valid result entry;
        # it simply gets no "rules" descriptor.
        finding = Finding(
            code="LWXXX",
            name="made-up",
            severity=Severity.ERROR,
            message="something",
            hint="fix it",
            location=Location(file="wf.py", line=1, column=1),
        )
        result = LintResult(findings=(finding,), files_scanned=1)
        document = json.loads(render_sarif(result))
        assert document["runs"][0]["tool"]["driver"]["rules"] == []
        assert document["runs"][0]["results"][0]["ruleId"] == "LWXXX"


class TestReporterRegistry:
    def test_all_three_formats_are_registered(self) -> None:
        assert set(REPORTERS) == {"text", "json", "sarif"}
