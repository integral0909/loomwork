"""Tests for :mod:`loomwork.visibility.typecheck`."""

from __future__ import annotations

from datetime import timedelta

import pytest

from loomwork.visibility.errors import QueryTypeError
from loomwork.visibility.nodes import Compare, TimestampLiteral
from loomwork.visibility.parser import parse_query
from loomwork.visibility.typecheck import check_query
from tests.visibility.support import NOW, SCHEMA


def check(source: str):
    return check_query(parse_query(source), SCHEMA, NOW, source)


def test_valid_query_round_trips() -> None:
    checked = check("status = 'failed' AND attr.customer_tier = 'gold'")
    assert checked.where is not None


def test_unknown_builtin_field() -> None:
    with pytest.raises(QueryTypeError, match="unknown field"):
        check("bogus_field = 'x'")


def test_unknown_search_attribute() -> None:
    with pytest.raises(QueryTypeError, match="unknown search attribute"):
        check("attr.bogus = 'x'")


def test_unknown_status_value() -> None:
    with pytest.raises(QueryTypeError, match="unknown status"):
        check("status = 'bogus'")


def test_valid_status_values_all_pass() -> None:
    for value in ("running", "completed", "failed", "canceled", "terminated"):
        check(f"status = '{value}'")


def test_type_mismatch_string_field_int_literal() -> None:
    with pytest.raises(QueryTypeError, match="is string but the value is int"):
        check("workflow_name = 5")


def test_int_field_rejects_float_literal() -> None:
    with pytest.raises(QueryTypeError, match="is int but the value is float"):
        check("attr.retries = 1.5")


def test_float_field_accepts_int_literal() -> None:
    check("attr.score = 5")


def test_like_requires_string_field() -> None:
    with pytest.raises(QueryTypeError, match="LIKE requires a string field"):
        check("tick_failures LIKE '5%'")


def test_like_on_string_field_is_fine() -> None:
    check("close_reason LIKE 'ops%'")


def test_contains_requires_keyword_list() -> None:
    with pytest.raises(QueryTypeError, match="CONTAINS requires a keyword-list attribute"):
        check("attr.customer_tier CONTAINS 'gold'")


def test_contains_on_keyword_list_is_fine() -> None:
    check("attr.tags CONTAINS 'urgent'")


def test_between_rejects_bool() -> None:
    with pytest.raises(QueryTypeError, match="BETWEEN is not supported"):
        check("attr.is_vip BETWEEN true AND false")


def test_between_rejects_keyword_list() -> None:
    with pytest.raises(QueryTypeError, match="BETWEEN is not supported"):
        check("attr.tags BETWEEN 'a' AND 'z'")


def test_between_on_orderable_types() -> None:
    check("tick_failures BETWEEN 1 AND 5")
    check("created_at BETWEEN timestamp '2024-01-01T00:00:00Z' AND now()")
    check("close_reason BETWEEN 'a' AND 'z'")


def test_is_null_rejects_non_nullable_builtin() -> None:
    with pytest.raises(QueryTypeError, match="is never null"):
        check("status IS NULL")


def test_is_null_allowed_on_nullable_builtin() -> None:
    check("closed_at IS NULL")
    check("last_tick_error IS NOT NULL")


def test_is_null_always_allowed_on_attrs() -> None:
    # attrs are always nullable, whatever their type
    check("attr.customer_tier IS NULL")
    check("attr.tags IS NOT NULL")


def test_order_by_rejects_search_attributes() -> None:
    # ORDER BY is restricted to built-in fields (see typecheck.py for why), even for an
    # attribute of an orderable type.
    with pytest.raises(QueryTypeError, match="only built-in fields"):
        check("ORDER BY attr.retries")
    with pytest.raises(QueryTypeError, match="only built-in fields"):
        check("ORDER BY attr.tags")


def test_order_by_rejects_bool() -> None:
    with pytest.raises(QueryTypeError, match="cannot ORDER BY"):
        check("ORDER BY cancel_requested")


def test_order_by_allows_orderable_builtin_fields() -> None:
    check("ORDER BY created_at DESC, workflow_name ASC")


def test_bare_interval_is_rejected() -> None:
    with pytest.raises(QueryTypeError, match="cannot be used by itself"):
        check("tick_failures = interval '1 day'")


def test_interval_arithmetic_requires_timestamp_base() -> None:
    # The parser only ever builds a TimeArith on top of now()/a timestamp literal, so this
    # defensive check is not reachable through parsed text; build the AST directly instead.
    from loomwork.visibility.nodes import Field, IntervalLiteral, IntLiteral, Query, TimeArith
    from tests.visibility.support import POS

    bogus_base = IntLiteral(1, POS)
    bogus = TimeArith(bogus_base, "+", IntervalLiteral(timedelta(days=1), "1 day", POS), POS)  # type: ignore[arg-type]
    query = Query(Compare(Field("tick_failures", False, POS), "=", bogus, POS), (), None)
    with pytest.raises(QueryTypeError, match="requires a timestamp"):
        check_query(query, SCHEMA, NOW)


def test_now_is_folded_to_a_concrete_timestamp() -> None:
    checked = check("created_at <= now()")
    assert isinstance(checked.where, Compare)
    assert isinstance(checked.where.value, TimestampLiteral)
    assert checked.where.value.value == NOW


def test_now_minus_interval_is_folded() -> None:
    checked = check("created_at >= now() - interval '1 day'")
    assert isinstance(checked.where, Compare)
    value = checked.where.value
    assert isinstance(value, TimestampLiteral)
    assert value.value == NOW - timedelta(days=1)


def test_null_literal_is_compatible_with_any_type() -> None:
    # `x = NULL` type-checks (it is always unknown at evaluation time, never a type error).
    check("attr.customer_tier = null")
    check("tick_failures = null")


def test_and_or_not_recurse_into_children() -> None:
    with pytest.raises(QueryTypeError, match="unknown field"):
        check("status = 'failed' AND (bogus = 'x' OR NOT bogus = 'y')")
