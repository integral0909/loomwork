"""Recursive-descent parser for the visibility query language.

Precedence, tightest first: ``NOT`` > ``AND`` > ``OR``, with parentheses overriding either.
This mirrors SQL's ``WHERE`` clause precedence, which is what operators writing these
queries are most likely to already know.

The parser also folds interval and timestamp *text* into concrete :class:`~datetime.timedelta`
/ :class:`~datetime.datetime` values as it builds the AST (``interval '1 day'`` becomes a
``timedelta`` immediately). What it does **not** fold is ``now()`` and time arithmetic on it
(``now() - interval '1 day'``): that needs an injected clock and is done later, by
:func:`loomwork.visibility.typecheck.check_query`, so query evaluation stays deterministic
and replayable.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from loomwork.visibility.errors import QuerySyntaxError
from loomwork.visibility.lexer import Position, Token, TokenKind, tokenize
from loomwork.visibility.nodes import (
    And,
    Between,
    BoolExpr,
    BoolLiteral,
    Compare,
    Contains,
    Field,
    FloatLiteral,
    InList,
    IntervalLiteral,
    IntLiteral,
    IsNull,
    Like,
    Literal,
    Not,
    NowLiteral,
    NullLiteral,
    Or,
    OrderTerm,
    Query,
    StringLiteral,
    TimeArith,
    TimestampLiteral,
)

#: Maximum nesting depth of boolean expressions (parentheses, AND/OR/NOT chains). Deeply
#: nested queries cost the parser and type checker recursion depth for no legitimate
#: operator benefit, so they are rejected outright.
MAX_EXPR_DEPTH = 64

#: Maximum number of literals inside a single ``IN (...)`` list.
MAX_IN_ITEMS = 500

#: Maximum accepted ``LIMIT`` value.
MAX_LIMIT = 10_000

_COMPARISON_OPS: dict[TokenKind, str] = {
    TokenKind.EQ: "=",
    TokenKind.NEQ: "!=",
    TokenKind.LT: "<",
    TokenKind.LTE: "<=",
    TokenKind.GT: ">",
    TokenKind.GTE: ">=",
}

_INTERVAL_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$")

#: Seconds per unit, keyed by every spelling accepted in an ``interval '...'`` literal.
_INTERVAL_UNITS: dict[str, float] = {}
for _names, _seconds in (
    (("w", "wk", "wks", "week", "weeks"), 7 * 86400),
    (("d", "day", "days"), 86400),
    (("h", "hr", "hrs", "hour", "hours"), 3600),
    (("m", "min", "mins", "minute", "minutes"), 60),
    (("s", "sec", "secs", "second", "seconds"), 1),
):
    for _name in _names:
        _INTERVAL_UNITS[_name] = _seconds


def parse_interval(text: str, position: Position, source: str) -> timedelta:
    """Parse an ``interval '...'`` body such as ``"1 day"`` or ``"2h"``."""
    match = _INTERVAL_RE.match(text)
    if match is None:
        raise QuerySyntaxError(
            f"invalid interval {text!r}: expected a number and a unit, e.g. '1 day'",
            source,
            position,
        )
    amount, unit = match.groups()
    seconds = _INTERVAL_UNITS.get(unit.lower())
    if seconds is None:
        valid = ", ".join(sorted({*_INTERVAL_UNITS}))
        raise QuerySyntaxError(
            f"invalid interval unit {unit!r}: expected one of {valid}", source, position
        )
    return timedelta(seconds=float(amount) * seconds)


def parse_timestamp(text: str, position: Position, source: str) -> datetime:
    """Parse a ``timestamp '...'`` body as ISO 8601, requiring an explicit timezone."""
    try:
        value = datetime.fromisoformat(text)
    except ValueError as exc:
        raise QuerySyntaxError(f"invalid timestamp {text!r}: {exc}", source, position) from exc
    if value.tzinfo is None:
        raise QuerySyntaxError(
            f"timestamp {text!r} has no timezone; use an offset or trailing 'Z'", source, position
        )
    return value


class _Parser:
    def __init__(self, source: str, tokens: list[Token]) -> None:
        self.source = source
        self.tokens = tokens
        self.pos = 0

    # ------------------------------------------------------------------ token helpers

    def _peek(self, offset: int = 0) -> Token:
        index = min(self.pos + offset, len(self.tokens) - 1)
        return self.tokens[index]

    def _advance(self) -> Token:
        tok = self.tokens[self.pos]
        if tok.kind is not TokenKind.EOF:
            self.pos += 1
        return tok

    def _match(self, kind: TokenKind) -> Token | None:
        if self._peek().kind is kind:
            return self._advance()
        return None

    def _match_keyword(self, *words: str) -> Token | None:
        if self._peek().is_keyword(*words):
            return self._advance()
        return None

    def _expect(self, kind: TokenKind, what: str) -> Token:
        tok = self._peek()
        if tok.kind is not kind:
            raise self._error(f"expected {what}, found {_describe(tok)}", tok.position)
        return self._advance()

    def _expect_keyword(self, *words: str) -> Token:
        tok = self._peek()
        if not tok.is_keyword(*words):
            expected = " or ".join(words)
            raise self._error(f"expected {expected}, found {_describe(tok)}", tok.position)
        return self._advance()

    def _error(self, message: str, position: Position) -> QuerySyntaxError:
        return QuerySyntaxError(message, self.source, position)

    def _check_depth(self, depth: int, position: Position) -> None:
        if depth > MAX_EXPR_DEPTH:
            raise self._error(f"query is nested more than {MAX_EXPR_DEPTH} levels deep", position)

    # ------------------------------------------------------------------ top level

    def parse(self) -> Query:
        where: BoolExpr | None = None
        if not self._peek().is_keyword("ORDER", "LIMIT") and self._peek().kind is not TokenKind.EOF:
            where = self._parse_or(0)
        order_by: tuple[OrderTerm, ...] = ()
        if self._match_keyword("ORDER"):
            self._expect_keyword("BY")
            order_by = self._parse_order_terms()
        limit: int | None = None
        if self._match_keyword("LIMIT"):
            limit = self._parse_limit()
        tail = self._peek()
        if tail.kind is not TokenKind.EOF:
            raise self._error(f"unexpected {_describe(tail)} after query", tail.position)
        return Query(where, order_by, limit)

    # ------------------------------------------------------------------ boolean expressions

    def _parse_or(self, depth: int) -> BoolExpr:
        left = self._parse_and(depth + 1)
        while True:
            tok = self._match_keyword("OR")
            if tok is None:
                return left
            right = self._parse_and(depth + 1)
            left = Or(left, right, tok.position)

    def _parse_and(self, depth: int) -> BoolExpr:
        left = self._parse_not(depth + 1)
        while True:
            tok = self._match_keyword("AND")
            if tok is None:
                return left
            right = self._parse_not(depth + 1)
            left = And(left, right, tok.position)

    def _parse_not(self, depth: int) -> BoolExpr:
        self._check_depth(depth, self._peek().position)
        tok = self._match_keyword("NOT")
        if tok is not None:
            operand = self._parse_not(depth + 1)
            return Not(operand, tok.position)
        return self._parse_primary(depth + 1)

    def _parse_primary(self, depth: int) -> BoolExpr:
        self._check_depth(depth, self._peek().position)
        if self._match(TokenKind.LPAREN):
            inner = self._parse_or(depth + 1)
            self._expect(TokenKind.RPAREN, "')'")
            return inner
        return self._parse_comparison()

    # ------------------------------------------------------------------ comparisons

    def _parse_field(self) -> Field:
        tok = self._expect(TokenKind.IDENT, "a field name")
        if tok.text.lower() == "attr":
            self._expect(TokenKind.DOT, "'.' after 'attr'")
            name_tok = self._expect(TokenKind.IDENT, "an attribute name")
            return Field(name_tok.text.lower(), True, tok.position)
        return Field(tok.text.lower(), False, tok.position)

    def _parse_comparison(self) -> BoolExpr:
        fld = self._parse_field()
        tok = self._peek()

        op = _COMPARISON_OPS.get(tok.kind)
        if op is not None:
            self._advance()
            value = self._parse_scalar()
            return Compare(fld, op, value, tok.position)

        negated = False
        if tok.is_keyword("NOT"):
            negated = True
            self._advance()
            tok = self._peek()

        if tok.is_keyword("IN"):
            self._advance()
            values = self._parse_scalar_list()
            return InList(fld, values, negated, tok.position)
        if tok.is_keyword("LIKE"):
            self._advance()
            pattern = self._parse_string_literal("a LIKE pattern")
            return Like(fld, pattern, negated, tok.position)
        if tok.is_keyword("BETWEEN"):
            self._advance()
            low = self._parse_scalar()
            self._expect_keyword("AND")
            high = self._parse_scalar()
            return Between(fld, low, high, negated, tok.position)
        if negated:
            raise self._error(
                f"expected IN, LIKE or BETWEEN after NOT, found {_describe(tok)}", tok.position
            )
        if tok.is_keyword("IS"):
            self._advance()
            is_negated = self._match_keyword("NOT") is not None
            self._expect_keyword("NULL")
            return IsNull(fld, is_negated, tok.position)
        if tok.is_keyword("CONTAINS"):
            self._advance()
            value = self._parse_string_literal("a CONTAINS value")
            return Contains(fld, value, tok.position)
        raise self._error(f"expected a comparison operator, found {_describe(tok)}", tok.position)

    def _parse_string_literal(self, what: str) -> StringLiteral:
        tok = self._expect(TokenKind.STRING, what)
        return StringLiteral(tok.value, tok.position)

    def _parse_scalar_list(self) -> tuple[Literal, ...]:
        self._expect(TokenKind.LPAREN, "'(' to start a list")
        values = [self._parse_scalar()]
        while self._match(TokenKind.COMMA):
            if len(values) >= MAX_IN_ITEMS:
                tok = self._peek()
                raise self._error(
                    f"IN list exceeds the maximum of {MAX_IN_ITEMS} values", tok.position
                )
            values.append(self._parse_scalar())
        self._expect(TokenKind.RPAREN, "')' to close a list")
        return tuple(values)

    # ------------------------------------------------------------------ scalars

    def _parse_scalar(self) -> Literal:
        tok = self._peek()
        if tok.kind is TokenKind.STRING:
            self._advance()
            return StringLiteral(tok.value, tok.position)
        if tok.kind is TokenKind.INTEGER:
            self._advance()
            return IntLiteral(tok.value, tok.position)
        if tok.kind is TokenKind.FLOAT:
            self._advance()
            return FloatLiteral(tok.value, tok.position)
        if tok.kind is TokenKind.MINUS and self._peek(1).kind in (
            TokenKind.INTEGER,
            TokenKind.FLOAT,
        ):
            self._advance()
            num = self._advance()
            if num.kind is TokenKind.INTEGER:
                return IntLiteral(-num.value, tok.position)
            return FloatLiteral(-num.value, tok.position)
        if tok.is_keyword("TRUE"):
            self._advance()
            return BoolLiteral(True, tok.position)
        if tok.is_keyword("FALSE"):
            self._advance()
            return BoolLiteral(False, tok.position)
        if tok.is_keyword("NULL"):
            self._advance()
            return NullLiteral(tok.position)
        if tok.is_keyword("NOW"):
            self._advance()
            self._expect(TokenKind.LPAREN, "'(' after now")
            self._expect(TokenKind.RPAREN, "')' after now(")
            return self._parse_time_arith(NowLiteral(tok.position))
        if tok.is_keyword("TIMESTAMP"):
            self._advance()
            text_tok = self._expect(TokenKind.STRING, "a quoted timestamp")
            value = parse_timestamp(text_tok.value, text_tok.position, self.source)
            return self._parse_time_arith(TimestampLiteral(value, tok.position))
        if tok.is_keyword("INTERVAL"):
            self._advance()
            text_tok = self._expect(TokenKind.STRING, "a quoted interval")
            delta = parse_interval(text_tok.value, text_tok.position, self.source)
            return IntervalLiteral(delta, text_tok.value, tok.position)
        raise self._error(f"expected a value, found {_describe(tok)}", tok.position)

    def _parse_time_arith(
        self, base: NowLiteral | TimestampLiteral | TimeArith
    ) -> NowLiteral | TimestampLiteral | TimeArith:
        node = base
        while self._peek().kind in (TokenKind.PLUS, TokenKind.MINUS):
            op_tok = self._advance()
            op = "+" if op_tok.kind is TokenKind.PLUS else "-"
            if not self._peek().is_keyword("INTERVAL"):
                raise self._error(
                    f"expected INTERVAL after {op!r}, found {_describe(self._peek())}",
                    self._peek().position,
                )
            self._advance()
            text_tok = self._expect(TokenKind.STRING, "a quoted interval")
            delta = parse_interval(text_tok.value, text_tok.position, self.source)
            interval = IntervalLiteral(delta, text_tok.value, text_tok.position)
            node = TimeArith(node, op, interval, op_tok.position)
        return node

    # ------------------------------------------------------------------ ORDER BY / LIMIT

    def _parse_order_terms(self) -> tuple[OrderTerm, ...]:
        terms = [self._parse_order_term()]
        while self._match(TokenKind.COMMA):
            terms.append(self._parse_order_term())
        return tuple(terms)

    def _parse_order_term(self) -> OrderTerm:
        fld = self._parse_field()
        descending = self._match_keyword("DESC") is not None
        if not descending:
            self._match_keyword("ASC")
        return OrderTerm(fld, descending)

    def _parse_limit(self) -> int:
        tok = self._expect(TokenKind.INTEGER, "an integer")
        if tok.value <= 0:
            raise self._error("LIMIT must be a positive integer", tok.position)
        if tok.value > MAX_LIMIT:
            raise self._error(f"LIMIT exceeds the maximum of {MAX_LIMIT}", tok.position)
        return int(tok.value)


def _describe(tok: Token) -> str:
    if tok.kind is TokenKind.EOF:
        return "end of query"
    if tok.kind is TokenKind.IDENT:
        return f"{tok.text!r}"
    return f"{tok.kind.value} {tok.text!r}" if tok.text else str(tok.kind.value)


def parse_query(source: str) -> Query:
    """Parse ``source`` into a :class:`~loomwork.visibility.nodes.Query`.

    Raises :class:`~loomwork.visibility.errors.QuerySyntaxError` on malformed input. The
    result still needs :func:`loomwork.visibility.typecheck.check_query` before it is safe
    to evaluate or compile: parsing alone does not know about search attribute types or
    resolve ``now()``.
    """
    tokens = tokenize(source)
    return _Parser(source, tokens).parse()


__all__ = [
    "MAX_EXPR_DEPTH",
    "MAX_IN_ITEMS",
    "MAX_LIMIT",
    "parse_interval",
    "parse_query",
    "parse_timestamp",
]
