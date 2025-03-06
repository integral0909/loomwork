"""Keeps ``docs/lint-rules.md`` honest: every registered rule must be documented."""

from __future__ import annotations

from pathlib import Path

from loomwork.lint.registry import DEFAULT_REGISTRY

_DOCS_PATH = Path(__file__).resolve().parents[2] / "docs" / "lint-rules.md"


class TestDocsCoverEveryRule:
    def test_every_rule_code_has_a_heading(self) -> None:
        text = _DOCS_PATH.read_text(encoding="utf-8")
        missing = [rule.code for rule in DEFAULT_REGISTRY.all() if f"### {rule.code}:" not in text]
        assert missing == []

    def test_every_rule_code_appears_in_the_summary_table(self) -> None:
        text = _DOCS_PATH.read_text(encoding="utf-8")
        missing = [rule.code for rule in DEFAULT_REGISTRY.all() if f"[{rule.code}]" not in text]
        assert missing == []
