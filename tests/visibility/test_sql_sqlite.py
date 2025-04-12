"""Verify the SQL compiler against a real SQLite database.

Every test here builds the actual proposed schema (migration 1's DDL plus
``lw_search_attributes``), inserts fixtures, compiles a query, runs it, and checks the
result against the in-memory evaluator applied to the same fixtures -- the two must always
agree (see ``docs/visibility-queries.md`` for why: both are meant to implement identical
SQL three-valued-logic semantics for the same checked query).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest

from loomwork.store.records import ExecutionRecord, ExecutionStatus
from loomwork.store.sql.migrations import Dialect
from loomwork.visibility.errors import QueryTypeError
from loomwork.visibility.evaluator import compile_predicate, order_and_limit
from loomwork.visibility.parser import parse_query
from loomwork.visibility.sql import compile_sql
from tests.visibility.support import (
    NOW,
    SCHEMA,
    build_sqlite_connection,
    insert_attrs,
    insert_execution,
    make_record,
    run_sql,
    ts_in,
)

#: A (record, search attributes) pair, as used throughout this file's fixture lists.
Fixture = tuple[ExecutionRecord, dict[str, Any]]


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    connection = build_sqlite_connection()
    yield connection
    connection.close()


def seed(conn: sqlite3.Connection, fixtures: list[Fixture]) -> None:
    """Insert ``fixtures`` (record, attrs pairs) into the database, once per test."""
    for rec, attrs in fixtures:
        insert_execution(conn, rec)
        insert_attrs(conn, rec.run_id, SCHEMA, attrs)
    conn.commit()


def run_and_check(
    conn: sqlite3.Connection, source: str, fixtures: list[Fixture], limit: int | None = None
) -> list[str]:
    """Run ``source`` both ways against already-``seed``-ed ``fixtures`` and assert agreement.

    Without ``ORDER BY``, row order is unspecified, so membership (as a set) is compared.
    With ``ORDER BY``, the exact row order (including any ``LIMIT``) must match too, since
    :func:`~loomwork.visibility.evaluator.order_and_limit` and the compiled ``ORDER BY`` /
    ``LIMIT`` clause are expected to always agree.
    """
    query = parse_query(source)
    predicate = compile_predicate(query, SCHEMA, NOW)
    matching_records = [rec for rec, attrs in fixtures if predicate(rec, attrs)]

    where_sql, params, order_by_sql = compile_sql(
        query, SCHEMA, dialect=Dialect.SQLITE, now=NOW, ts_in=ts_in
    )
    actual_order = run_sql(conn, where_sql, params, order_by_sql, limit=limit or query.limit)

    if query.order_by:
        expected_order = [r.run_id for r in order_and_limit(matching_records, query, SCHEMA, NOW)]
        assert actual_order == expected_order, (source, actual_order, expected_order)
    else:
        expected = {r.run_id for r in matching_records}
        assert set(actual_order) == expected, (source, actual_order, expected)
    return actual_order


class TestBuiltinFields:
    def test_equality(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="a", status=ExecutionStatus.FAILED), {}),
            (make_record(run_id="b", status=ExecutionStatus.RUNNING), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "status = 'failed'", fixtures)

    def test_comparison_operators(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id=str(n), tick_failures=n), {}) for n in range(5)
        ]
        seed(conn, fixtures)
        for op in ("=", "!=", "<", "<=", ">", ">="):
            run_and_check(conn, f"tick_failures {op} 2", fixtures)

    def test_null_closed_at(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="open", closed_at=None), {}),
            (make_record(run_id="closed", closed_at=NOW), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "closed_at IS NULL", fixtures)
        run_and_check(conn, "closed_at IS NOT NULL", fixtures)
        # A comparison against a NULL column is unknown -- never true -- matching the SQL
        # engine's own NULL handling exactly (no rows should ever match either query below).
        run_and_check(conn, "closed_at = timestamp '2024-06-15T12:00:00Z'", fixtures)
        run_and_check(conn, "closed_at != timestamp '2024-06-15T12:00:00Z'", fixtures)

    def test_like_with_percent_and_underscore(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="a", close_reason="ops-timeout"), {}),
            (make_record(run_id="b", close_reason="sys-error"), {}),
            (make_record(run_id="c", close_reason="ops1"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "close_reason LIKE 'ops%'", fixtures)
        run_and_check(conn, "close_reason NOT LIKE 'ops%'", fixtures)
        run_and_check(conn, "close_reason LIKE 'ops_'", fixtures)

    def test_like_escapes_literal_wildcards(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="literal", close_reason="50%off"), {}),
            (make_record(run_id="other", close_reason="50xoff"), {}),
            (make_record(run_id="under", close_reason="a_b"), {}),
            (make_record(run_id="notunder", close_reason="axb"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, r"close_reason LIKE '50\%off'", fixtures)
        run_and_check(conn, r"close_reason LIKE 'a\_b'", fixtures)

    def test_in_and_between(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id=str(n), tick_failures=n), {}) for n in range(6)
        ]
        seed(conn, fixtures)
        run_and_check(conn, "tick_failures IN (1, 3, 5)", fixtures)
        run_and_check(conn, "tick_failures NOT IN (1, 3, 5)", fixtures)
        run_and_check(conn, "tick_failures BETWEEN 2 AND 4", fixtures)
        run_and_check(conn, "tick_failures NOT BETWEEN 2 AND 4", fixtures)

    def test_and_or_not(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="a", status=ExecutionStatus.FAILED, close_reason="ops-x"), {}),
            (make_record(run_id="b", status=ExecutionStatus.FAILED, close_reason="sys-x"), {}),
            (make_record(run_id="c", status=ExecutionStatus.RUNNING, close_reason="ops-x"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "status = 'failed' AND NOT (close_reason LIKE 'ops%')", fixtures)
        run_and_check(conn, "status = 'failed' OR close_reason LIKE 'ops%'", fixtures)

    def test_now_and_interval(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="recent", created_at=NOW - timedelta(hours=2)), {}),
            (make_record(run_id="old", created_at=NOW - timedelta(days=3)), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "created_at >= now() - interval '1 day'", fixtures)


class TestSearchAttributes:
    def test_scalar_equality(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="gold"), {"customer_tier": "gold"}),
            (make_record(run_id="silver"), {"customer_tier": "silver"}),
            (make_record(run_id="unset"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.customer_tier = 'gold'", fixtures)
        run_and_check(conn, "attr.customer_tier != 'gold'", fixtures)

    def test_scalar_is_null(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="set"), {"customer_tier": "gold"}),
            (make_record(run_id="unset"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.customer_tier IS NULL", fixtures)
        run_and_check(conn, "attr.customer_tier IS NOT NULL", fixtures)

    def test_in_list(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="gold"), {"customer_tier": "gold"}),
            (make_record(run_id="plat"), {"customer_tier": "platinum"}),
            (make_record(run_id="bronze"), {"customer_tier": "bronze"}),
            (make_record(run_id="unset"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.customer_tier IN ('gold', 'platinum')", fixtures)

    def test_numeric_and_float(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id=str(n)), {"retries": n, "score": n + 0.5}) for n in range(5)
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.retries BETWEEN 1 AND 3", fixtures)
        run_and_check(conn, "attr.score > 2.5", fixtures)

    def test_bool_attribute(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="vip"), {"is_vip": True}),
            (make_record(run_id="not_vip"), {"is_vip": False}),
            (make_record(run_id="unset"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.is_vip = true", fixtures)
        run_and_check(conn, "attr.is_vip = false", fixtures)

    def test_datetime_attribute(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="recent"), {"activated_at": NOW - timedelta(hours=1)}),
            (make_record(run_id="old"), {"activated_at": NOW - timedelta(days=30)}),
            (make_record(run_id="unset"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.activated_at >= now() - interval '1 day'", fixtures)

    def test_contains_keyword_list(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="a"), {"tags": ["urgent", "billing"]}),
            (make_record(run_id="b"), {"tags": ["billing"]}),
            (make_record(run_id="c"), {}),  # attribute entirely unset
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.tags CONTAINS 'urgent'", fixtures)
        run_and_check(conn, "NOT (attr.tags CONTAINS 'urgent')", fixtures)

    def test_keyword_list_is_null(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="set"), {"tags": ["a", "b"]}),
            (make_record(run_id="unset"), {}),
        ]
        seed(conn, fixtures)
        run_and_check(conn, "attr.tags IS NULL", fixtures)
        run_and_check(conn, "attr.tags IS NOT NULL", fixtures)

    def test_mixed_builtin_and_attr(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (
                make_record(
                    run_id="a", status=ExecutionStatus.FAILED, workflow_name="orders.process"
                ),
                {"customer_tier": "gold"},
            ),
            (
                make_record(
                    run_id="b", status=ExecutionStatus.FAILED, workflow_name="orders.process"
                ),
                {"customer_tier": "bronze"},
            ),
            (
                make_record(
                    run_id="c", status=ExecutionStatus.RUNNING, workflow_name="orders.process"
                ),
                {"customer_tier": "gold"},
            ),
        ]
        seed(conn, fixtures)
        run_and_check(
            conn,
            "status = 'failed' AND workflow_name = 'orders.process' "
            "AND attr.customer_tier IN ('gold', 'platinum')",
            fixtures,
        )


class TestOrderByAndLimit:
    def test_order_by_builtin_field(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id=str(n), tick_failures=n), {}) for n in (3, 1, 2)
        ]
        seed(conn, fixtures)
        order = run_and_check(conn, "ORDER BY tick_failures", fixtures)
        assert order == ["1", "2", "3"]

    def test_order_by_desc_with_limit(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id=str(n), tick_failures=n), {}) for n in range(5)
        ]
        seed(conn, fixtures)
        order = run_and_check(conn, "ORDER BY tick_failures DESC LIMIT 2", fixtures)
        assert order == ["4", "3"]

    def test_order_by_rejects_search_attributes(self, conn: sqlite3.Connection) -> None:
        # ORDER BY is restricted to built-in fields (see typecheck.py); the SQL compiler
        # never even sees an attribute in ORDER BY because check_query rejects it first.
        with pytest.raises(QueryTypeError, match="only built-in fields"):
            compile_sql(
                parse_query("ORDER BY attr.retries"),
                SCHEMA,
                dialect=Dialect.SQLITE,
                now=NOW,
                ts_in=ts_in,
            )

    def test_nulls_sort_last_both_directions(self, conn: sqlite3.Connection) -> None:
        fixtures: list[Fixture] = [
            (make_record(run_id="a", closed_at=NOW), {}),
            (make_record(run_id="b", closed_at=None), {}),
            (make_record(run_id="c", closed_at=NOW - timedelta(hours=1)), {}),
        ]
        seed(conn, fixtures)
        assert run_and_check(conn, "ORDER BY closed_at ASC", fixtures) == ["c", "a", "b"]
        assert run_and_check(conn, "ORDER BY closed_at DESC", fixtures) == ["a", "c", "b"]
