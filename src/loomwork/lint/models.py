"""Data types shared by the analyzer, the rules and the reporters.

Keeping these as plain, immutable dataclasses (rather than passing tuples or dicts
around) means every layer of the linter agrees on one vocabulary: a :class:`Finding` is
what a rule reports, a :class:`Location` is where it happened, and a :class:`LintResult`
is what a full run produces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    """How serious a finding is.

    ``ERROR`` findings are almost certainly wrong in a workflow (they read the wall
    clock, perform I/O, ...). ``WARNING`` findings are suspicious but sometimes
    intentional (logging with ``print``, reading an environment variable that is
    expected to be stable for the process lifetime, ...).
    """

    ERROR = "error"
    WARNING = "warning"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value


@dataclass(slots=True, frozen=True, order=True)
class Location:
    """A position in a source file.

    ``line`` and ``column`` are 1-indexed, matching the convention used by editors,
    compilers and every other Python lint tool. ``end_line``/``end_column`` are ``None``
    when the underlying AST node predates Python's end-position tracking or when a
    finding has no natural span (for example a whole-file suppression).
    """

    file: str
    line: int
    column: int
    end_line: int | None = None
    end_column: int | None = None

    def __str__(self) -> str:
        return f"{self.file}:{self.line}:{self.column}"


@dataclass(slots=True, frozen=True)
class Finding:
    """One reported determinism violation (or parse error)."""

    code: str
    name: str
    severity: Severity
    message: str
    hint: str
    location: Location
    #: Names of the functions between the workflow entry point and the function that
    #: contains this finding, in call order, when the violation was reached through an
    #: intra-module call rather than found directly in the workflow body. Empty when the
    #: violation is directly in the workflow function.
    call_path: tuple[str, ...] = field(default_factory=tuple)

    def sort_key(self) -> tuple[str, int, int, str]:
        return (self.location.file, self.location.line, self.location.column, self.code)


@dataclass(slots=True, frozen=True)
class LintResult:
    """The outcome of linting one or more files."""

    findings: tuple[Finding, ...]
    files_scanned: int

    @property
    def error_count(self) -> int:
        return sum(1 for f in self.findings if f.severity is Severity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.severity is Severity.WARNING)

    @property
    def has_findings(self) -> bool:
        return bool(self.findings)

    def exit_code(self) -> int:
        """The process exit code this result corresponds to (0 clean, 1 findings)."""
        return 1 if self.has_findings else 0
