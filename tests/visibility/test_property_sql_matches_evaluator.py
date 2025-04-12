"""Property: the in-memory evaluator and the SQL compiler always agree.

This is the key correctness guarantee for the visibility query language: whatever a query
means, it must mean the same thing whether it runs in-process or against a database. Both
:func:`~loomwork.visibility.evaluator.compile_predicate` and
:func:`~loomwork.visibility.sql.compile_sql` are independent implementations of the same
checked :class:`~loomwork.visibility.nodes.Query`; Hypothesis explores randomly generated
queries and fixtures (including NULLs, in every combination the type checker allows) looking
for a case where they disagree.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from loomwork.store.records import ExecutionRecord
from loomwork.store.sql.migrations import Dialect
from loomwork.visibility.evaluator import compile_predicate, order_and_limit
from loomwork.visibility.nodes import BoolExpr, OrderTerm, Query
from loomwork.visibility.sql import compile_sql
from tests.visibility.strategies import bool_exprs, order_terms, record_batches
from tests.visibility.support import (
    NOW,
    SCHEMA,
    build_sqlite_connection,
    insert_attrs,
    insert_execution,
    run_sql,
    ts_in,
)

pytestmark = pytest.mark.slow

#: A (record, search attributes) pair, matching tests/visibility/support.py's fixtures.
Fixture = tuple[ExecutionRecord, dict[str, Any]]

_SETTINGS = settings(
    max_examples=300,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)


def _seed(batch: list[Fixture]) -> sqlite3.Connection:
    conn = build_sqlite_connection()
    for rec, attrs in batch:
        insert_execution(conn, rec)
        insert_attrs(conn, rec.run_id, SCHEMA, attrs)
    conn.commit()
    return conn


@given(batch=record_batches(), where=bool_exprs())
@_SETTINGS
def test_where_clause_matches_between_evaluator_and_sqlite(
    batch: list[Fixture], where: BoolExpr
) -> None:
    query = Query(where, (), None)
    predicate = compile_predicate(query, SCHEMA, NOW)
    expected = {rec.run_id for rec, attrs in batch if predicate(rec, attrs)}

    conn = _seed(batch)
    try:
        where_sql, params, order_by_sql = compile_sql(
            query, SCHEMA, dialect=Dialect.SQLITE, now=NOW, ts_in=ts_in
        )
        actual = set(run_sql(conn, where_sql, params, order_by_sql))
    finally:
        conn.close()

    assert actual == expected


@given(
    batch=record_batches(),
    where=st.one_of(st.none(), bool_exprs()),
    order_by=order_terms(),
    limit=st.one_of(st.none(), st.integers(min_value=1, max_value=10)),
)
@_SETTINGS
def test_order_by_and_limit_match_between_evaluator_and_sqlite(
    batch: list[Fixture],
    where: BoolExpr | None,
    order_by: tuple[OrderTerm, ...],
    limit: int | None,
) -> None:
    query = Query(where, order_by, limit)
    predicate = compile_predicate(query, SCHEMA, NOW)
    matching = [rec for rec, attrs in batch if predicate(rec, attrs)]
    expected_order = [r.run_id for r in order_and_limit(matching, query, SCHEMA, NOW)]

    conn = _seed(batch)
    try:
        where_sql, params, order_by_sql = compile_sql(
            query, SCHEMA, dialect=Dialect.SQLITE, now=NOW, ts_in=ts_in
        )
        actual_order = run_sql(conn, where_sql, params, order_by_sql, limit=limit)
    finally:
        conn.close()

    assert actual_order == expected_order
