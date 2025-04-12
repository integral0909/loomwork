"""Tests for the in-memory evaluator, with emphasis on three-valued NULL semantics."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from loomwork.store.records import ExecutionRecord, ExecutionStatus
from loomwork.visibility.evaluator import compile_predicate, order_and_limit
from loomwork.visibility.parser import parse_query
from tests.visibility.support import NOW, SCHEMA, make_record


def matches(source: str, record: ExecutionRecord, attrs: dict[str, Any] | None = None) -> bool:
    query = parse_query(source)
    predicate = compile_predicate(query, SCHEMA, NOW)
    return predicate(record, attrs or {})


class TestBasicOperators:
    def test_equality(self) -> None:
        rec = make_record(workflow_name="orders.process")
        assert matches("workflow_name = 'orders.process'", rec)
        assert not matches("workflow_name = 'other'", rec)

    def test_inequality(self) -> None:
        rec = make_record(workflow_name="orders.process")
        assert matches("workflow_name != 'other'", rec)

    def test_ordering_operators(self) -> None:
        rec = make_record(tick_failures=3)
        assert matches("tick_failures > 1", rec)
        assert matches("tick_failures >= 3", rec)
        assert matches("tick_failures < 5", rec)
        assert matches("tick_failures <= 3", rec)
        assert not matches("tick_failures > 3", rec)

    def test_in_list(self) -> None:
        rec = make_record(status=ExecutionStatus.FAILED)
        assert matches("status IN ('failed', 'canceled')", rec)
        assert not matches("status IN ('completed', 'canceled')", rec)
        assert matches("status NOT IN ('completed', 'canceled')", rec)

    def test_between(self) -> None:
        rec = make_record(tick_failures=3)
        assert matches("tick_failures BETWEEN 1 AND 5", rec)
        assert not matches("tick_failures BETWEEN 4 AND 5", rec)
        assert matches("tick_failures NOT BETWEEN 4 AND 5", rec)

    def test_status_field(self) -> None:
        rec = make_record(status=ExecutionStatus.RUNNING)
        assert matches("status = 'running'", rec)


class TestLike:
    def test_percent_wildcard(self) -> None:
        rec = make_record(close_reason="ops-timeout")
        assert matches("close_reason LIKE 'ops%'", rec)
        assert not matches("close_reason LIKE 'sys%'", rec)

    def test_underscore_wildcard(self) -> None:
        rec = make_record(close_reason="ops1")
        assert matches("close_reason LIKE 'ops_'", rec)
        assert not matches("close_reason LIKE 'ops__'", rec)

    def test_escaped_wildcard_is_literal(self) -> None:
        rec_literal = make_record(close_reason="50%")
        rec_other = make_record(close_reason="50x")
        assert matches(r"close_reason LIKE '50\%'", rec_literal)
        assert not matches(r"close_reason LIKE '50\%'", rec_other)

    def test_not_like(self) -> None:
        rec = make_record(close_reason="ops-timeout")
        assert not matches("close_reason NOT LIKE 'ops%'", rec)
        assert matches("close_reason NOT LIKE 'sys%'", rec)


class TestContains:
    def test_contains_match(self) -> None:
        rec = make_record()
        assert matches("attr.tags CONTAINS 'urgent'", rec, {"tags": ["urgent", "billing"]})

    def test_contains_no_match(self) -> None:
        rec = make_record()
        assert not matches("attr.tags CONTAINS 'urgent'", rec, {"tags": ["billing"]})

    def test_contains_missing_attribute_is_unknown(self) -> None:
        rec = make_record()
        assert not matches("attr.tags CONTAINS 'urgent'", rec, {})
        # NOT of an unknown result is still unknown -- excluded either way.
        assert not matches("NOT (attr.tags CONTAINS 'urgent')", rec, {})


class TestNullSemantics:
    """SQL three-valued logic: comparisons against NULL are unknown, never true or false."""

    def test_missing_attr_makes_equality_unknown(self) -> None:
        rec = make_record()
        assert not matches("attr.customer_tier = 'gold'", rec, {})
        assert not matches("attr.customer_tier != 'gold'", rec, {})

    def test_null_builtin_field(self) -> None:
        rec = make_record(closed_at=None)
        assert not matches("closed_at = timestamp '2024-01-01T00:00:00Z'", rec)
        assert not matches("closed_at != timestamp '2024-01-01T00:00:00Z'", rec)

    def test_is_null_is_never_unknown(self) -> None:
        rec = make_record()
        assert matches("attr.customer_tier IS NULL", rec, {})
        assert not matches("attr.customer_tier IS NOT NULL", rec, {})
        rec_with_attr = make_record()
        assert not matches("attr.customer_tier IS NULL", rec_with_attr, {"customer_tier": "gold"})

    def test_not_of_unknown_is_unknown(self) -> None:
        rec = make_record()
        # NOT(NULL) is NULL, which a WHERE clause excludes just like False.
        assert not matches("NOT (attr.customer_tier = 'gold')", rec, {})

    def test_and_with_unknown_and_false_is_false(self) -> None:
        rec = make_record(status=ExecutionStatus.RUNNING)
        # unknown AND false == false: the whole row is excluded (as it should be).
        assert not matches("attr.customer_tier = 'gold' AND status = 'failed'", rec, {})

    def test_and_with_unknown_and_true_is_unknown(self) -> None:
        rec = make_record(status=ExecutionStatus.FAILED)
        # unknown AND true == unknown: still excluded.
        assert not matches("attr.customer_tier = 'gold' AND status = 'failed'", rec, {})

    def test_or_with_unknown_and_true_is_true(self) -> None:
        rec = make_record(status=ExecutionStatus.FAILED)
        # unknown OR true == true.
        assert matches("attr.customer_tier = 'gold' OR status = 'failed'", rec, {})

    def test_or_with_unknown_and_false_is_unknown(self) -> None:
        rec = make_record(status=ExecutionStatus.RUNNING)
        assert not matches("attr.customer_tier = 'gold' OR status = 'failed'", rec, {})

    def test_like_against_null_is_unknown(self) -> None:
        rec = make_record()
        assert not matches("last_tick_error LIKE '%boom%'", rec)
        assert not matches("last_tick_error NOT LIKE '%boom%'", rec)

    def test_between_with_null_bound_is_unknown(self) -> None:
        rec = make_record()
        assert not matches("attr.retries BETWEEN 1 AND 5", rec, {})

    def test_in_list_containing_null_is_unknown_not_false(self) -> None:
        rec = make_record()
        # `x IN (a, NULL)`: no match among non-null candidates, but NULL in the list means
        # "unknown", not "false" -- same as SQL.
        assert not matches("attr.customer_tier IN ('gold', null)", rec, {"customer_tier": "silver"})
        assert matches("attr.customer_tier IN ('gold', null)", rec, {"customer_tier": "gold"})

    def test_equals_null_literal_is_always_unknown(self) -> None:
        rec = make_record()
        assert not matches("attr.customer_tier = null", rec, {"customer_tier": "gold"})


class TestOrderAndLimit:
    def test_orders_ascending_by_default(self) -> None:
        recs = [make_record(tick_failures=n) for n in (3, 1, 2)]
        query = parse_query("ORDER BY tick_failures")
        ordered = order_and_limit(recs, query, SCHEMA, NOW)
        assert [r.tick_failures for r in ordered] == [1, 2, 3]

    def test_orders_descending(self) -> None:
        recs = [make_record(tick_failures=n) for n in (3, 1, 2)]
        query = parse_query("ORDER BY tick_failures DESC")
        ordered = order_and_limit(recs, query, SCHEMA, NOW)
        assert [r.tick_failures for r in ordered] == [3, 2, 1]

    def test_nulls_sort_last_regardless_of_direction(self) -> None:
        recs = [
            make_record(run_id="a", closed_at=NOW),
            make_record(run_id="b", closed_at=None),
            make_record(run_id="c", closed_at=NOW - timedelta(hours=1)),
        ]
        asc = order_and_limit(recs, parse_query("ORDER BY closed_at ASC"), SCHEMA, NOW)
        assert [r.run_id for r in asc] == ["c", "a", "b"]
        desc = order_and_limit(recs, parse_query("ORDER BY closed_at DESC"), SCHEMA, NOW)
        assert [r.run_id for r in desc] == ["a", "c", "b"]

    def test_multiple_order_terms(self) -> None:
        recs = [
            make_record(run_id="a", workflow_name="w1", tick_failures=2),
            make_record(run_id="b", workflow_name="w1", tick_failures=1),
            make_record(run_id="c", workflow_name="w2", tick_failures=0),
        ]
        query = parse_query("ORDER BY workflow_name, tick_failures DESC")
        ordered = order_and_limit(recs, query, SCHEMA, NOW)
        assert [r.run_id for r in ordered] == ["a", "b", "c"]

    def test_limit_truncates(self) -> None:
        recs = [make_record(tick_failures=n) for n in range(5)]
        query = parse_query("ORDER BY tick_failures LIMIT 2")
        ordered = order_and_limit(recs, query, SCHEMA, NOW)
        assert [r.tick_failures for r in ordered] == [0, 1]

    def test_no_order_by_preserves_input_order(self) -> None:
        recs = [make_record(run_id=str(n)) for n in range(3)]
        query = parse_query("")
        result = order_and_limit(recs, query, SCHEMA, NOW)
        assert [r.run_id for r in result] == ["0", "1", "2"]


def test_predicate_with_no_where_matches_everything() -> None:
    predicate = compile_predicate(parse_query(""), SCHEMA, NOW)
    assert predicate(make_record(), {})


@pytest.mark.parametrize("op", ["=", "!=", "<", "<=", ">", ">="])
def test_all_comparison_operators_parse_and_run(op: str) -> None:
    rec = make_record(tick_failures=3)
    matches(f"tick_failures {op} 3", rec)
