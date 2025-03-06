"""Configuration: ``[tool.loomwork.lint]`` in ``pyproject.toml``.

.. code-block:: toml

    [tool.loomwork.lint]
    select = ["LW101", "LW102"]           # only these codes; omit to run everything
    ignore = ["LW104"]                    # never report these codes
    extend-forbidden-calls = ["myapp.legacy.fetch"]  # extra forbidden dotted calls
    allowed-calls = ["requests.get"]      # exempt a normally-forbidden dotted call
    exclude = ["**/generated/**"]         # glob patterns of files to skip entirely

All keys are optional; an absent table means "use the defaults" (every rule enabled,
nothing excluded). Everything is validated eagerly so a typo in a rule code fails fast
with a clear message instead of silently matching nothing.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loomwork.lint.errors import ConfigError
from loomwork.lint.registry import DEFAULT_REGISTRY, RuleRegistry

_DOTTED_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$")


@dataclass(slots=True, frozen=True)
class LintConfig:
    """Validated linter configuration."""

    select: frozenset[str] | None = None
    ignore: frozenset[str] = field(default_factory=frozenset)
    extend_forbidden_calls: tuple[str, ...] = ()
    allowed_calls: frozenset[str] = field(default_factory=frozenset)
    exclude: tuple[str, ...] = ()

    def is_enabled(self, code: str) -> bool:
        if code in self.ignore:
            return False
        if self.select is None:
            return True
        return code in self.select

    @classmethod
    def default(cls) -> LintConfig:
        return cls()

    @classmethod
    def from_mapping(
        cls, data: Mapping[str, Any], *, registry: RuleRegistry = DEFAULT_REGISTRY
    ) -> LintConfig:
        """Build and validate a config from a ``[tool.loomwork.lint]``-shaped mapping."""
        known = set(registry.codes()) | {"LW199"}  # LW199 covers extend-forbidden-calls
        select = _optional_code_set(data, "select", known)
        ignore = _code_set(data, "ignore", known)
        extend = _dotted_name_tuple(data, "extend-forbidden-calls")
        allowed = frozenset(_dotted_name_tuple(data, "allowed-calls"))
        exclude = _string_tuple(data, "exclude")
        _reject_unknown_keys(
            data, {"select", "ignore", "extend-forbidden-calls", "allowed-calls", "exclude"}
        )
        return cls(
            select=select,
            ignore=ignore,
            extend_forbidden_calls=extend,
            allowed_calls=allowed,
            exclude=exclude,
        )

    @classmethod
    def from_pyproject(
        cls, path: str | Path, *, registry: RuleRegistry = DEFAULT_REGISTRY
    ) -> LintConfig:
        """Load ``[tool.loomwork.lint]`` from a ``pyproject.toml`` file.

        A missing file, or a file with no ``[tool.loomwork.lint]`` table, yields the
        default configuration rather than an error: most projects will never customize
        the linter.
        """
        path = Path(path)
        if not path.is_file():
            return cls.default()
        try:
            data = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"{path}: invalid TOML: {exc}") from exc
        section = data.get("tool", {}).get("loomwork", {}).get("lint", {})
        if not isinstance(section, dict):
            raise ConfigError(f"{path}: [tool.loomwork.lint] must be a table")
        try:
            return cls.from_mapping(section, registry=registry)
        except ConfigError as exc:
            raise ConfigError(f"{path}: {exc}") from exc


def _reject_unknown_keys(data: Mapping[str, Any], known_keys: set[str]) -> None:
    unknown = sorted(set(data) - known_keys)
    if unknown:
        raise ConfigError(
            f"unknown [tool.loomwork.lint] key(s): {', '.join(unknown)}; "
            f"expected one of {', '.join(sorted(known_keys))}"
        )


def _code_set(data: Mapping[str, Any], key: str, known: set[str]) -> frozenset[str]:
    result = _optional_code_set(data, key, known)
    return result if result is not None else frozenset()


def _optional_code_set(data: Mapping[str, Any], key: str, known: set[str]) -> frozenset[str] | None:
    if key not in data:
        return None
    raw = data[key]
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise ConfigError(f"{key!r} must be a list of rule codes")
    unknown = sorted(set(raw) - known)
    if unknown:
        raise ConfigError(f"unknown rule code(s) in {key!r}: {', '.join(unknown)}")
    return frozenset(raw)


def _dotted_name_tuple(data: Mapping[str, Any], key: str) -> tuple[str, ...]:
    if key not in data:
        return ()
    raw = data[key]
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise ConfigError(f"{key!r} must be a list of dotted names")
    invalid = [item for item in raw if not _DOTTED_NAME_RE.match(item)]
    if invalid:
        raise ConfigError(
            f"{key!r} entries must be dotted names like 'module.function': {', '.join(invalid)}"
        )
    return tuple(raw)


def _string_tuple(data: Mapping[str, Any], key: str) -> tuple[str, ...]:
    if key not in data:
        return ()
    raw = data[key]
    if not isinstance(raw, list) or not all(isinstance(item, str) and item for item in raw):
        raise ConfigError(f"{key!r} must be a list of non-empty strings")
    return tuple(raw)
