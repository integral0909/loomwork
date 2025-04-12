"""Errors raised while parsing, checking or compiling a visibility query.

Both error types carry a :class:`~loomwork.visibility.lexer.Position` so a caller (a CLI,
an HTTP handler) can point the operator at the exact place the query went wrong. ``format``
renders the offending line with a caret, the way compilers usually do::

    status = 'bogus'
             ^
    unknown status 'bogus': expected one of completed, failed, ...
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loomwork.visibility.lexer import Position


class VisibilityQueryError(Exception):
    """Base class for errors raised by :mod:`loomwork.visibility`."""

    def __init__(self, message: str, source: str, position: Position) -> None:
        super().__init__(message)
        self.message = message
        self.source = source
        self.position = position

    def format(self) -> str:
        """Render the error with the source line and a caret at the offending column."""
        lines = self.source.splitlines() or [""]
        line_index = min(self.position.line - 1, len(lines) - 1)
        line_index = max(line_index, 0)
        line_text = lines[line_index] if lines else ""
        caret = " " * max(self.position.column - 1, 0) + "^"
        return f"{line_text}\n{caret}\n{self.message}"

    def __str__(self) -> str:
        return f"{self.message} (line {self.position.line}, column {self.position.column})"


class QuerySyntaxError(VisibilityQueryError):
    """The query text could not be lexed or parsed."""


class QueryTypeError(VisibilityQueryError):
    """The query parsed but does not type-check against the schema in use."""


__all__ = ["QuerySyntaxError", "QueryTypeError", "VisibilityQueryError"]
