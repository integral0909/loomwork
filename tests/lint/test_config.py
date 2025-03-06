"""Configuration parsing and validation for ``[tool.loomwork.lint]``."""

from __future__ import annotations

from pathlib import Path

import pytest

from loomwork.lint.config import LintConfig
from loomwork.lint.errors import ConfigError
from tests.lint.helpers import codes_of, lint


class TestDefaults:
    def test_default_config_enables_everything(self) -> None:
        config = LintConfig.default()
        assert config.is_enabled("LW101")
        assert config.is_enabled("anything")
        assert config.select is None
        assert config.ignore == frozenset()
        assert config.exclude == ()

    def test_missing_pyproject_yields_defaults(self, tmp_path: Path) -> None:
        config = LintConfig.from_pyproject(tmp_path / "does-not-exist.toml")
        assert config == LintConfig.default()

    def test_pyproject_without_the_lint_table_yields_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "pyproject.toml"
        path.write_text("[tool.other]\nx = 1\n")
        config = LintConfig.from_pyproject(path)
        assert config == LintConfig.default()


class TestSelectAndIgnore:
    def test_select_restricts_to_listed_codes(self) -> None:
        config = LintConfig.from_mapping({"select": ["LW101"]})
        assert config.is_enabled("LW101")
        assert not config.is_enabled("LW102")

    def test_ignore_excludes_listed_codes(self) -> None:
        config = LintConfig.from_mapping({"ignore": ["LW104"]})
        assert not config.is_enabled("LW104")
        assert config.is_enabled("LW101")

    def test_unknown_code_in_select_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError, match="LW999"):
            LintConfig.from_mapping({"select": ["LW999"]})

    def test_unknown_code_in_ignore_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError, match="LW999"):
            LintConfig.from_mapping({"ignore": ["LW999"]})

    def test_select_and_ignore_end_to_end(self) -> None:
        config = LintConfig.from_mapping({"ignore": ["LW104"]})
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                print("hi")
            """,
            config,
        )
        assert findings == []


class TestExtendForbiddenCalls:
    def test_extended_call_is_flagged_under_lw199(self) -> None:
        config = LintConfig.from_mapping({"extend-forbidden-calls": ["myapp.legacy.fetch"]})
        findings = lint(
            """
            import myapp.legacy
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                myapp.legacy.fetch()
            """,
            config,
        )
        assert codes_of(findings) == ["LW199"]

    def test_malformed_dotted_name_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError, match="dotted"):
            LintConfig.from_mapping({"extend-forbidden-calls": ["not_dotted"]})

    def test_non_string_entry_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError, match="dotted names"):
            LintConfig.from_mapping({"extend-forbidden-calls": [123]})

    def test_non_list_value_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError, match="dotted names"):
            LintConfig.from_mapping({"allowed-calls": "time.time"})


class TestAllowedCalls:
    def test_allowed_call_exempts_a_normally_forbidden_call(self) -> None:
        config = LintConfig.from_mapping({"allowed-calls": ["time.time"]})
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.time()
            """,
            config,
        )
        assert findings == []

    def test_allowed_calls_does_not_exempt_other_calls(self) -> None:
        config = LintConfig.from_mapping({"allowed-calls": ["time.time"]})
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.sleep(1)
            """,
            config,
        )
        assert codes_of(findings) == ["LW101"]


class TestExclude:
    def test_exclude_is_stored_as_a_tuple(self) -> None:
        config = LintConfig.from_mapping({"exclude": ["**/generated/**"]})
        assert config.exclude == ("**/generated/**",)

    def test_exclude_must_be_a_list_of_non_empty_strings(self) -> None:
        with pytest.raises(ConfigError):
            LintConfig.from_mapping({"exclude": [""]})
        with pytest.raises(ConfigError):
            LintConfig.from_mapping({"exclude": "not-a-list"})


class TestValidationErrors:
    def test_unknown_key_is_a_config_error(self) -> None:
        with pytest.raises(ConfigError, match="unknown"):
            LintConfig.from_mapping({"selct": ["LW101"]})

    def test_select_must_be_a_list(self) -> None:
        with pytest.raises(ConfigError):
            LintConfig.from_mapping({"select": "LW101"})

    def test_invalid_toml_is_a_config_error(self, tmp_path: Path) -> None:
        path = tmp_path / "pyproject.toml"
        path.write_text("this is not [ valid toml\n")
        with pytest.raises(ConfigError, match="invalid TOML"):
            LintConfig.from_pyproject(path)

    def test_lint_table_must_be_a_table(self, tmp_path: Path) -> None:
        path = tmp_path / "pyproject.toml"
        path.write_text("[tool.loomwork]\nlint = 'nope'\n")
        with pytest.raises(ConfigError, match="must be a table"):
            LintConfig.from_pyproject(path)


class TestLoadFromPyproject:
    def test_reads_the_lint_table(self, tmp_path: Path) -> None:
        path = tmp_path / "pyproject.toml"
        path.write_text(
            "[tool.loomwork.lint]\n" 'ignore = ["LW104"]\n' 'exclude = ["**/generated/**"]\n'
        )
        config = LintConfig.from_pyproject(path)
        assert config.ignore == frozenset({"LW104"})
        assert config.exclude == ("**/generated/**",)
