"""Shared fixtures for the visibility test suite: a schema, a SQLite harness and factories.

The SQLite harness builds the *real* migration DDL for ``lw_executions``
(``loomwork.store.sql.migrations``) plus the proposed ``lw_search_attributes`` DDL
(``loomwork.visibility.ddl``) in an in-memory database, so tests exercise
:func:`~loomwork.visibility.sql.compile_sql` against the schema it is actually meant for.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from loomwork.store.records import ExecutionRecord, ExecutionStatus
from loomwork.store.sql.migrations import MIGRATIONS, Dialect
from loomwork.visibility.ddl import SEARCH_ATTRIBUTES_MIGRATION, VALUE_COLUMNS, VALUE_TYPES
from loomwork.visibility.lexer import Position
from loomwork.visibility.schema import AttributeType, SearchAttributes

#: A dummy position for AST nodes built directly by tests rather than via the parser.
#: Positions are excluded from equality/hashing (see nodes.py) so any value is fine here.
POS = Position(1, 1, 0)

NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

SCHEMA = SearchAttributes.define(
    customer_tier=AttributeType.STRING,
    retries=AttributeType.INT,
    score=AttributeType.FLOAT,
    is_vip=AttributeType.BOOL,
    activated_at=AttributeType.DATETIME,
    tags=AttributeType.KEYWORD_LIST,
)

# Mirrors SQLStore.SQLiteStore's own timestamp representation -- integer microseconds since
# the epoch (see ADR-0003 and the ``lw_executions``/``lw_search_attributes`` DDL, whose
# SQLite columns are all INTEGER). Reimplemented here, rather than instantiated from
# ``loomwork.store.sqlite.SQLiteStore``, so these tests do not depend on that class being
# fully constructible (it may gain unrelated abstract methods as the store subsystem grows).
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICRO = timedelta(microseconds=1)


def ts_in(value: datetime) -> int:
    return (value - _EPOCH) // _MICRO


def ts_out(value: Any) -> datetime:
    return _EPOCH + timedelta(microseconds=int(value))


def make_record(
    *,
    run_id: str | None = None,
    workflow_name: str = "orders.process",
    status: ExecutionStatus = ExecutionStatus.RUNNING,
    created_at: datetime = NOW,
    closed_at: datetime | None = None,
    tick_failures: int = 0,
    cancel_requested: bool = False,
    close_reason: str = "",
    last_tick_error: str | None = None,
) -> ExecutionRecord:
    return ExecutionRecord(
        run_id=run_id or f"run-{uuid.uuid4().hex[:8]}",
        workflow_id=f"wf-{uuid.uuid4().hex[:8]}",
        workflow_name=workflow_name,
        status=status,
        created_at=created_at,
        closed_at=closed_at,
        tick_failures=tick_failures,
        cancel_requested=cancel_requested,
        close_reason=close_reason,
        last_tick_error=last_tick_error,
    )


def build_sqlite_connection() -> sqlite3.Connection:
    """An in-memory database with the real ``lw_executions`` schema plus search attributes."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    for statement in MIGRATIONS[0].statements(Dialect.SQLITE):
        conn.execute(statement)
    for statement in SEARCH_ATTRIBUTES_MIGRATION.statements(Dialect.SQLITE):
        conn.execute(statement)
    conn.commit()
    return conn


def insert_execution(conn: sqlite3.Connection, rec: ExecutionRecord) -> None:
    conn.execute(
        """
        INSERT INTO lw_executions (
            run_id, workflow_id, workflow_name, status, created_at, closed_at,
            tick_failures, cancel_requested, close_reason, last_tick_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rec.run_id,
            rec.workflow_id,
            rec.workflow_name,
            rec.status.value,
            ts_in(rec.created_at),
            ts_in(rec.closed_at) if rec.closed_at is not None else None,
            rec.tick_failures,
            int(rec.cancel_requested),
            rec.close_reason,
            rec.last_tick_error,
        ),
    )


def insert_attrs(
    conn: sqlite3.Connection, run_id: str, schema: SearchAttributes, attrs: Mapping[str, Any]
) -> None:
    for name, value in attrs.items():
        if value is None:
            continue
        attr_type = schema.type_of(name)
        assert attr_type is not None, f"{name!r} is not in the test schema"
        schema.validate_value(name, value)
        value_type = VALUE_TYPES[attr_type.value]
        if attr_type is AttributeType.KEYWORD_LIST:
            # An empty list writes zero rows, which is indistinguishable from "never set"
            # (see the "Known limitation" note in loomwork/visibility/ddl.py) -- callers
            # should not pass one, and the property test's generator never does either.
            for element in value:
                conn.execute(
                    "INSERT INTO lw_search_attributes (run_id, name, value_type, str_value) "
                    "VALUES (?, ?, ?, ?)",
                    (run_id, name, value_type, element),
                )
            continue
        col = VALUE_COLUMNS[attr_type.value]
        stored: Any
        if attr_type is AttributeType.DATETIME:
            stored = ts_in(value)
        elif attr_type is AttributeType.BOOL:
            stored = int(value)
        else:
            stored = value
        conn.execute(
            f"INSERT INTO lw_search_attributes (run_id, name, value_type, {col}) "
            "VALUES (?, ?, ?, ?)",
            (run_id, name, value_type, stored),
        )


def run_sql(
    conn: sqlite3.Connection,
    where_sql: str,
    params: list[Any],
    order_by_sql: str,
    limit: int | None = None,
) -> list[str]:
    """Run a compiled WHERE/ORDER BY against ``lw_executions`` and return matching run ids."""
    sql = f"SELECT run_id FROM lw_executions WHERE {where_sql}"
    all_params = list(params)
    if order_by_sql:
        sql += f" ORDER BY {order_by_sql}"
    if limit is not None:
        sql += " LIMIT ?"
        all_params.append(limit)
    rows = conn.execute(sql, all_params).fetchall()
    return [row["run_id"] for row in rows]
