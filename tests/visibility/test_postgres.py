"""Verify the SQL compiler's ``postgres`` dialect against a real PostgreSQL server.

Skipped unless ``LOOMWORK_TEST_POSTGRES_DSN`` is set. Uses a dedicated, dropped-at-the-end
schema so it never collides with other tests' or engineers' use of the same database, and
talks to psycopg directly (rather than through ``loomwork.store.postgres.PostgresStore``) so
it stays independent of that class's own construction requirements.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest

from loomwork.store.records import ExecutionRecord, ExecutionStatus
from loomwork.store.sql.migrations import MIGRATIONS, Dialect
from loomwork.visibility.ddl import SEARCH_ATTRIBUTES_MIGRATION, VALUE_COLUMNS, VALUE_TYPES
from loomwork.visibility.evaluator import compile_predicate
from loomwork.visibility.parser import parse_query
from loomwork.visibility.schema import AttributeType
from loomwork.visibility.sql import compile_sql
from tests.visibility.support import NOW, SCHEMA, make_record

if TYPE_CHECKING:
    from psycopg import Connection

#: A (record, search attributes) pair, as used throughout this file's fixture lists.
Fixture = tuple[ExecutionRecord, dict[str, Any]]

DSN = os.environ.get("LOOMWORK_TEST_POSTGRES_DSN", "")

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not DSN, reason="set LOOMWORK_TEST_POSTGRES_DSN to run PostgreSQL tests"),
]

_SCHEMA_NAME = "lw_visibility_test"


def _to_psycopg(sql: str) -> str:
    # The compiler only ever emits '?' placeholders (never a literal '%'), so this is a safe,
    # one-directional translation -- the same one loomwork.store.postgres uses internally.
    return sql.replace("%", "%%").replace("?", "%s")


@pytest.fixture
def conn() -> Iterator[Connection[Any]]:
    import psycopg

    connection = psycopg.connect(DSN, autocommit=True)
    connection.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA_NAME} CASCADE")
    connection.execute(f"CREATE SCHEMA {_SCHEMA_NAME}")
    connection.execute(f"SET search_path TO {_SCHEMA_NAME}")
    for statement in MIGRATIONS[0].statements(Dialect.POSTGRES):
        connection.execute(statement)
    for statement in SEARCH_ATTRIBUTES_MIGRATION.statements(Dialect.POSTGRES):
        connection.execute(statement)
    try:
        yield connection
    finally:
        connection.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA_NAME} CASCADE")
        connection.close()


def ts_in(value: datetime) -> datetime:
    return value


def insert_execution(conn: Connection[Any], rec: ExecutionRecord) -> None:
    conn.execute(
        "INSERT INTO lw_executions (run_id, workflow_id, workflow_name, status, created_at, "
        "closed_at, tick_failures, cancel_requested, close_reason, last_tick_error) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            rec.run_id,
            rec.workflow_id,
            rec.workflow_name,
            rec.status.value,
            rec.created_at,
            rec.closed_at,
            rec.tick_failures,
            rec.cancel_requested,
            rec.close_reason,
            rec.last_tick_error,
        ),
    )


def insert_attrs(conn: Connection[Any], run_id: str, attrs: dict[str, Any]) -> None:
    for name, value in attrs.items():
        if value is None:
            continue
        attr_type = SCHEMA.type_of(name)
        assert attr_type is not None
        value_type = VALUE_TYPES[attr_type.value]
        if attr_type is AttributeType.KEYWORD_LIST:
            for element in value:
                conn.execute(
                    "INSERT INTO lw_search_attributes (run_id, name, value_type, str_value) "
                    "VALUES (%s, %s, %s, %s)",
                    (run_id, name, value_type, element),
                )
            continue
        col = VALUE_COLUMNS[attr_type.value]
        conn.execute(
            f"INSERT INTO lw_search_attributes (run_id, name, value_type, {col}) "
            "VALUES (%s, %s, %s, %s)",
            (run_id, name, value_type, value),
        )


def run_and_check(conn: Connection[Any], source: str, fixtures: list[Fixture]) -> None:
    query = parse_query(source)
    predicate = compile_predicate(query, SCHEMA, NOW)
    expected = {rec.run_id for rec, attrs in fixtures if predicate(rec, attrs)}

    where_sql, params, order_by_sql = compile_sql(
        query, SCHEMA, dialect=Dialect.POSTGRES, now=NOW, ts_in=ts_in
    )
    sql = f"SELECT run_id FROM lw_executions WHERE {where_sql}"
    if order_by_sql:
        sql += f" ORDER BY {order_by_sql}"
    rows = conn.execute(_to_psycopg(sql), params).fetchall()
    actual = {row[0] for row in rows}
    assert actual == expected, (source, actual, expected)


def test_builtin_field_equality(conn: Connection[Any]) -> None:
    fixtures: list[Fixture] = [
        (make_record(run_id="a", status=ExecutionStatus.FAILED), {}),
        (make_record(run_id="b", status=ExecutionStatus.RUNNING), {}),
    ]
    for rec, attrs in fixtures:
        insert_execution(conn, rec)
        insert_attrs(conn, rec.run_id, attrs)
    run_and_check(conn, "status = 'failed'", fixtures)


def test_null_semantics_and_search_attributes(conn: Connection[Any]) -> None:
    fixtures: list[Fixture] = [
        (make_record(run_id="gold", closed_at=None), {"customer_tier": "gold"}),
        (make_record(run_id="silver", closed_at=NOW), {"customer_tier": "silver"}),
        (make_record(run_id="unset", closed_at=None), {}),
    ]
    for rec, attrs in fixtures:
        insert_execution(conn, rec)
        insert_attrs(conn, rec.run_id, attrs)
    run_and_check(conn, "attr.customer_tier = 'gold'", fixtures)
    run_and_check(conn, "attr.customer_tier IS NULL", fixtures)
    run_and_check(conn, "closed_at IS NULL", fixtures)
    run_and_check(conn, "NOT (attr.customer_tier = 'gold')", fixtures)


def test_like_escaping(conn: Connection[Any]) -> None:
    fixtures: list[Fixture] = [
        (make_record(run_id="literal", close_reason="50%off"), {}),
        (make_record(run_id="other", close_reason="50xoff"), {}),
    ]
    for rec, attrs in fixtures:
        insert_execution(conn, rec)
        insert_attrs(conn, rec.run_id, attrs)
    run_and_check(conn, r"close_reason LIKE '50\%off'", fixtures)


def test_contains_keyword_list(conn: Connection[Any]) -> None:
    fixtures: list[Fixture] = [
        (make_record(run_id="a"), {"tags": ["urgent", "billing"]}),
        (make_record(run_id="b"), {"tags": ["billing"]}),
        (make_record(run_id="c"), {}),
    ]
    for rec, attrs in fixtures:
        insert_execution(conn, rec)
        insert_attrs(conn, rec.run_id, attrs)
    run_and_check(conn, "attr.tags CONTAINS 'urgent'", fixtures)
