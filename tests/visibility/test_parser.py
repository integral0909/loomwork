"""Tests for the recursive-descent parser: grammar shape, precedence and limits."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from loomwork.visibility.errors import QuerySyntaxError
from loomwork.visibility.nodes import (
    And,
    Between,
    Compare,
    Contains,
    Field,
    InList,
    IsNull,
    Like,
    Not,
    NowLiteral,
    Or,
    TimeArith,
    TimestampLiteral,
)
from loomwork.visibility.parser import (
    MAX_EXPR_DEPTH,
    MAX_IN_ITEMS,
    MAX_LIMIT,
    parse_interval,
    parse_query,
    parse_timestamp,
)


def test_simple_comparison() -> None:
    q = parse_query("status = 'failed'")
    assert isinstance(q.where, Compare)
    assert q.where.field == Field("status", False, q.where.field.position)
    assert q.where.op == "="
    assert q.where.value.value == "failed"  # type: ignore[union-attr]


def test_attr_field_is_lowercased_and_dotted() -> None:
    q = parse_query("attr.Customer_Tier = 'gold'")
    assert isinstance(q.where, Compare)
    assert q.where.field.is_attr is True
    assert q.where.field.name == "customer_tier"


def test_precedence_not_and_or() -> None:
    # NOT binds tighter than AND, which binds tighter than OR:
    # a OR b AND NOT c  ==  a OR (b AND (NOT c))
    q = parse_query("status = 'a' OR status = 'b' AND NOT status = 'c'")
    assert isinstance(q.where, Or)
    assert isinstance(q.where.right, And)
    assert isinstance(q.where.right.right, Not)


def test_parentheses_override_precedence() -> None:
    q = parse_query("(status = 'a' OR status = 'b') AND status = 'c'")
    assert isinstance(q.where, And)
    assert isinstance(q.where.left, Or)


def test_in_and_not_in() -> None:
    q = parse_query("status IN ('failed', 'canceled')")
    assert isinstance(q.where, InList)
    assert q.where.negated is False
    assert [v.value for v in q.where.values] == ["failed", "canceled"]  # type: ignore[union-attr]

    q2 = parse_query("status NOT IN ('failed', 'canceled')")
    assert isinstance(q2.where, InList)
    assert q2.where.negated is True


def test_like_and_not_like() -> None:
    q = parse_query("close_reason LIKE 'ops%'")
    assert isinstance(q.where, Like)
    assert q.where.negated is False

    q2 = parse_query("NOT (close_reason LIKE 'ops%')")
    assert isinstance(q2.where, Not)
    assert isinstance(q2.where.operand, Like)

    q3 = parse_query("close_reason NOT LIKE 'ops%'")
    assert isinstance(q3.where, Like)
    assert q3.where.negated is True


def test_is_null_and_is_not_null() -> None:
    q = parse_query("closed_at IS NULL")
    assert isinstance(q.where, IsNull)
    assert q.where.negated is False

    q2 = parse_query("closed_at IS NOT NULL")
    assert isinstance(q2.where, IsNull)
    assert q2.where.negated is True


def test_between_and_not_between() -> None:
    q = parse_query("tick_failures BETWEEN 1 AND 5")
    assert isinstance(q.where, Between)
    assert q.where.negated is False

    q2 = parse_query("tick_failures NOT BETWEEN 1 AND 5")
    assert isinstance(q2.where, Between)
    assert q2.where.negated is True


def test_contains() -> None:
    q = parse_query("attr.tags CONTAINS 'urgent'")
    assert isinstance(q.where, Contains)
    assert q.where.value.value == "urgent"


def test_now_and_interval_arithmetic() -> None:
    q = parse_query("created_at >= now() - interval '1 day'")
    assert isinstance(q.where, Compare)
    value = q.where.value
    assert isinstance(value, TimeArith)
    assert isinstance(value.left, NowLiteral)
    assert value.op == "-"
    assert value.interval.delta == timedelta(days=1)


def test_chained_time_arithmetic() -> None:
    q = parse_query("created_at >= now() - interval '1 day' + interval '2 hours'")
    assert isinstance(q.where, Compare)
    outer = q.where.value
    assert isinstance(outer, TimeArith)
    assert outer.op == "+"
    assert outer.interval.delta == timedelta(hours=2)
    assert isinstance(outer.left, TimeArith)
    assert outer.left.op == "-"


def test_timestamp_literal() -> None:
    q = parse_query("created_at = timestamp '2024-05-01T00:00:00Z'")
    assert isinstance(q.where, Compare)
    assert isinstance(q.where.value, TimestampLiteral)
    assert q.where.value.value == datetime(2024, 5, 1, tzinfo=UTC)


def test_order_by_and_limit() -> None:
    q = parse_query("status = 'failed' ORDER BY created_at DESC, workflow_name LIMIT 10")
    assert q.where is not None
    assert len(q.order_by) == 2
    assert q.order_by[0].field.name == "created_at"
    assert q.order_by[0].descending is True
    assert q.order_by[1].descending is False
    assert q.limit == 10


def test_order_by_and_limit_without_where() -> None:
    q = parse_query("ORDER BY created_at LIMIT 5")
    assert q.where is None
    assert q.limit == 5


def test_empty_query_is_valid() -> None:
    q = parse_query("")
    assert q.where is None
    assert q.order_by == ()
    assert q.limit is None


def test_negative_number_literal() -> None:
    q = parse_query("tick_failures = -1")
    assert isinstance(q.where, Compare)
    assert q.where.value.value == -1  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "source",
    [
        "status = ",
        "status =",
        "status 'failed'",
        "(status = 'failed'",
        "status = 'failed') ",
        "status IN 'failed'",
        "status LIKE 5",
        "status BETWEEN 1",
        "NOT",
        "status = 'a' AND",
        "attr customer_tier = 'x'",
        "attr. = 'x'",
        "status = 'a' status = 'b'",
    ],
)
def test_syntax_errors(source: str) -> None:
    with pytest.raises(QuerySyntaxError):
        parse_query(source)


def test_limit_must_be_positive() -> None:
    with pytest.raises(QuerySyntaxError, match="positive"):
        parse_query("LIMIT 0")


def test_limit_has_a_maximum() -> None:
    with pytest.raises(QuerySyntaxError, match="maximum"):
        parse_query(f"LIMIT {MAX_LIMIT + 1}")


def test_in_list_has_a_maximum_size() -> None:
    values = ", ".join(f"'{i}'" for i in range(MAX_IN_ITEMS + 1))
    with pytest.raises(QuerySyntaxError, match="maximum"):
        parse_query(f"status IN ({values})")


def test_deeply_nested_parens_are_rejected() -> None:
    source = "(" * (MAX_EXPR_DEPTH * 2) + "status = 'a'" + ")" * (MAX_EXPR_DEPTH * 2)
    with pytest.raises(QuerySyntaxError, match="nested"):
        parse_query(source)


def test_moderately_nested_parens_are_fine() -> None:
    depth = 5
    source = "(" * depth + "status = 'a'" + ")" * depth
    q = parse_query(source)
    assert isinstance(q.where, Compare)


def test_interval_units() -> None:
    from loomwork.visibility.lexer import Position

    pos = Position(1, 1, 0)
    assert parse_interval("1 day", pos, "") == timedelta(days=1)
    assert parse_interval("90 minutes", pos, "") == timedelta(minutes=90)
    assert parse_interval("2h", pos, "") == timedelta(hours=2)
    assert parse_interval("1.5 hours", pos, "") == timedelta(hours=1.5)


def test_invalid_interval_unit() -> None:
    from loomwork.visibility.lexer import Position

    with pytest.raises(QuerySyntaxError, match="invalid interval unit"):
        parse_interval("1 fortnight", Position(1, 1, 0), "")


def test_invalid_interval_shape() -> None:
    from loomwork.visibility.lexer import Position

    with pytest.raises(QuerySyntaxError, match="invalid interval"):
        parse_interval("day", Position(1, 1, 0), "")


def test_timestamp_without_timezone_is_rejected() -> None:
    from loomwork.visibility.lexer import Position

    with pytest.raises(QuerySyntaxError, match="timezone"):
        parse_timestamp("2024-05-01T00:00:00", Position(1, 1, 0), "")


def test_bare_interval_parses_but_is_rejected_later_by_typecheck() -> None:
    # Grammatically an interval can stand alone as a scalar; it is check_query's job to
    # reject it (see test_typecheck.py), not the parser's.
    from loomwork.visibility.nodes import IntervalLiteral

    q = parse_query("tick_failures = interval '1 day'")
    assert isinstance(q.where, Compare)
    assert isinstance(q.where.value, IntervalLiteral)
