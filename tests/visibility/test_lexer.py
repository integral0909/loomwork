"""Tests for the tokenizer."""

from __future__ import annotations

import pytest

from loomwork.visibility.errors import QuerySyntaxError
from loomwork.visibility.lexer import MAX_TOKENS, TokenKind, tokenize


def _kinds(source: str) -> list[TokenKind]:
    return [t.kind for t in tokenize(source)]


def test_empty_source_is_just_eof() -> None:
    assert _kinds("") == [TokenKind.EOF]
    assert _kinds("   \n\t  ") == [TokenKind.EOF]


def test_identifiers_and_dotted_attr() -> None:
    tokens = tokenize("attr.customer_tier")
    assert [t.kind for t in tokens] == [
        TokenKind.IDENT,
        TokenKind.DOT,
        TokenKind.IDENT,
        TokenKind.EOF,
    ]
    assert tokens[0].text == "attr"
    assert tokens[2].text == "customer_tier"


def test_string_literal_with_escaped_quote() -> None:
    tokens = tokenize("'it''s ok'")
    assert tokens[0].kind is TokenKind.STRING
    assert tokens[0].value == "it's ok"


def test_unterminated_string_raises() -> None:
    with pytest.raises(QuerySyntaxError, match="unterminated string"):
        tokenize("'oops")


def test_integer_and_float_literals() -> None:
    tokens = tokenize("42 3.14 2e10 1.5e-3")
    assert tokens[0].kind is TokenKind.INTEGER
    assert tokens[0].value == 42
    assert tokens[1].kind is TokenKind.FLOAT
    assert tokens[1].value == pytest.approx(3.14)
    assert tokens[2].kind is TokenKind.FLOAT
    assert tokens[2].value == pytest.approx(2e10)
    assert tokens[3].kind is TokenKind.FLOAT
    assert tokens[3].value == pytest.approx(1.5e-3)


def test_operators() -> None:
    tokens = tokenize("= != < <= > >= ( ) , + -")
    assert [t.kind for t in tokens[:-1]] == [
        TokenKind.EQ,
        TokenKind.NEQ,
        TokenKind.LT,
        TokenKind.LTE,
        TokenKind.GT,
        TokenKind.GTE,
        TokenKind.LPAREN,
        TokenKind.RPAREN,
        TokenKind.COMMA,
        TokenKind.PLUS,
        TokenKind.MINUS,
    ]


def test_bang_without_equals_is_an_error() -> None:
    with pytest.raises(QuerySyntaxError, match="expected '='"):
        tokenize("status ! 'x'")


def test_unexpected_character_reports_position() -> None:
    with pytest.raises(QuerySyntaxError) as exc_info:
        tokenize("status = @")
    err = exc_info.value
    assert err.position.column == 10
    assert "unexpected character" in err.message


def test_positions_track_lines_and_columns() -> None:
    tokens = tokenize("a\nb")
    assert tokens[0].position.line == 1
    assert tokens[0].position.column == 1
    assert tokens[1].position.line == 2
    assert tokens[1].position.column == 1


def test_is_keyword_is_case_insensitive() -> None:
    tokens = tokenize("AnD")
    assert tokens[0].is_keyword("AND")


def test_too_many_tokens_is_rejected() -> None:
    source = " ".join(["a"] * (MAX_TOKENS + 1))
    with pytest.raises(QuerySyntaxError, match="more than"):
        tokenize(source)


def test_query_error_format_has_caret() -> None:
    with pytest.raises(QuerySyntaxError) as exc_info:
        tokenize("status = @")
    rendered = exc_info.value.format()
    lines = rendered.splitlines()
    assert lines[0] == "status = @"
    assert lines[1] == " " * 9 + "^"
