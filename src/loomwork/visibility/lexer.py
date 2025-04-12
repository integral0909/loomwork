"""Tokenizer for the visibility query language.

The lexer knows nothing about grammar: it turns source text into a flat list of tokens
with positions, and reports illegal characters or unterminated literals. Keywords (``AND``,
``LIKE``, ``ORDER``, ...) are not a distinct token kind; they are plain :data:`TokenKind.IDENT`
tokens that the parser recognizes case-insensitively. This keeps the lexer small and lets
the parser produce keyword-aware error messages ("expected AND, OR or end of query").
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any

from loomwork.visibility.errors import QuerySyntaxError

#: Refuse queries longer than this many characters outright, before lexing.
MAX_QUERY_LENGTH = 8192

#: Refuse to tokenize past this many tokens. Guards against pathological input (for example
#: a query built from thousands of concatenated ``OR`` clauses) consuming unbounded memory
#: and CPU in the parser and type checker.
MAX_TOKENS = 4000


class TokenKind(enum.Enum):
    IDENT = "identifier"
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DOT = "'.'"
    LPAREN = "'('"
    RPAREN = "')'"
    COMMA = "','"
    EQ = "'='"
    NEQ = "'!='"
    LT = "'<'"
    LTE = "'<='"
    GT = "'>'"
    GTE = "'>='"
    PLUS = "'+'"
    MINUS = "'-'"
    EOF = "end of query"


@dataclass(frozen=True, slots=True)
class Position:
    """A location in the original query text, 1-based like most editors."""

    line: int
    column: int
    offset: int

    def __str__(self) -> str:
        return f"line {self.line}, column {self.column}"


@dataclass(frozen=True, slots=True)
class Token:
    kind: TokenKind
    text: str
    value: Any
    position: Position

    def is_keyword(self, *keywords: str) -> bool:
        """Whether this is an identifier matching one of ``keywords``, case-insensitively."""
        return self.kind is TokenKind.IDENT and self.text.upper() in keywords


_SINGLE_CHAR_TOKENS: dict[str, TokenKind] = {
    ".": TokenKind.DOT,
    "(": TokenKind.LPAREN,
    ")": TokenKind.RPAREN,
    ",": TokenKind.COMMA,
    "+": TokenKind.PLUS,
    "-": TokenKind.MINUS,
}


class _Scanner:
    """Single-pass, single-use scanner over the query text."""

    def __init__(self, source: str) -> None:
        self.source = source
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: list[Token] = []

    def _here(self) -> Position:
        return Position(self.line, self.column, self.pos)

    def _advance(self) -> str:
        ch = self.source[self.pos]
        self.pos += 1
        if ch == "\n":
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        return ch

    def _peek(self, offset: int = 0) -> str:
        index = self.pos + offset
        return self.source[index] if index < len(self.source) else ""

    def _error(self, message: str, position: Position | None = None) -> QuerySyntaxError:
        return QuerySyntaxError(message, self.source, position or self._here())

    def _emit(self, kind: TokenKind, text: str, value: Any, start: Position) -> None:
        if len(self.tokens) >= MAX_TOKENS:
            raise self._error(f"query has more than {MAX_TOKENS} tokens", start)
        self.tokens.append(Token(kind, text, value, start))

    def run(self) -> list[Token]:
        while True:
            self._skip_whitespace()
            if self.pos >= len(self.source):
                break
            start = self._here()
            ch = self._peek()
            if ch == "'":
                self._scan_string(start)
            elif ch.isdigit():
                self._scan_number(start)
            elif ch.isalpha() or ch == "_":
                self._scan_ident(start)
            elif ch in _SINGLE_CHAR_TOKENS:
                self._advance()
                self._emit(_SINGLE_CHAR_TOKENS[ch], ch, None, start)
            elif ch == "=":
                self._advance()
                self._emit(TokenKind.EQ, "=", None, start)
            elif ch == "!":
                self._advance()
                if self._peek() != "=":
                    raise self._error("expected '=' after '!'", start)
                self._advance()
                self._emit(TokenKind.NEQ, "!=", None, start)
            elif ch == "<":
                self._advance()
                if self._peek() == "=":
                    self._advance()
                    self._emit(TokenKind.LTE, "<=", None, start)
                else:
                    self._emit(TokenKind.LT, "<", None, start)
            elif ch == ">":
                self._advance()
                if self._peek() == "=":
                    self._advance()
                    self._emit(TokenKind.GTE, ">=", None, start)
                else:
                    self._emit(TokenKind.GT, ">", None, start)
            else:
                raise self._error(f"unexpected character {ch!r}", start)
        self._emit(TokenKind.EOF, "", None, self._here())
        return self.tokens

    def _skip_whitespace(self) -> None:
        while self.pos < len(self.source) and self._peek() in " \t\r\n":
            self._advance()

    def _scan_string(self, start: Position) -> None:
        self._advance()  # opening quote
        chars: list[str] = []
        while True:
            if self.pos >= len(self.source):
                raise self._error("unterminated string literal", start)
            ch = self._advance()
            if ch == "'":
                if self._peek() == "'":  # '' escapes a literal quote
                    self._advance()
                    chars.append("'")
                    continue
                break
            chars.append(ch)
        text = self.source[start.offset : self.pos]
        self._emit(TokenKind.STRING, text, "".join(chars), start)

    def _scan_number(self, start: Position) -> None:
        while self._peek().isdigit():
            self._advance()
        is_float = False
        if self._peek() == "." and self._peek(1).isdigit():
            is_float = True
            self._advance()
            while self._peek().isdigit():
                self._advance()
        exponent_marker = self._peek()
        after_marker = self._peek(1)
        if exponent_marker in ("e", "E") and (after_marker.isdigit() or after_marker in ("+", "-")):
            is_float = True
            self._advance()
            if self._peek() in ("+", "-"):
                self._advance()
            if not self._peek().isdigit():
                raise self._error("malformed exponent in numeric literal", start)
            while self._peek().isdigit():
                self._advance()
        text = self.source[start.offset : self.pos]
        if is_float:
            self._emit(TokenKind.FLOAT, text, float(text), start)
        else:
            self._emit(TokenKind.INTEGER, text, int(text), start)

    def _scan_ident(self, start: Position) -> None:
        while self._peek().isalnum() or self._peek() == "_":
            self._advance()
        text = self.source[start.offset : self.pos]
        self._emit(TokenKind.IDENT, text, None, start)


def tokenize(source: str) -> list[Token]:
    """Tokenize ``source``, raising :class:`QuerySyntaxError` on invalid input."""
    if len(source) > MAX_QUERY_LENGTH:
        raise QuerySyntaxError(
            f"query exceeds the maximum length of {MAX_QUERY_LENGTH} characters",
            source,
            Position(1, 1, 0),
        )
    return _Scanner(source).run()


__all__ = ["MAX_QUERY_LENGTH", "MAX_TOKENS", "Position", "Token", "TokenKind", "tokenize"]
