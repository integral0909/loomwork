"""Sanity checks for the proposed ``lw_search_attributes`` migration itself."""

from __future__ import annotations

import sqlite3

import pytest

from loomwork.store.sql.migrations import Dialect
from loomwork.visibility.ddl import SEARCH_ATTRIBUTES_MIGRATION, VALUE_COLUMNS, VALUE_TYPES
from loomwork.visibility.schema import AttributeType


def test_migration_has_both_dialects() -> None:
    assert SEARCH_ATTRIBUTES_MIGRATION.sqlite
    assert SEARCH_ATTRIBUTES_MIGRATION.postgres
    assert SEARCH_ATTRIBUTES_MIGRATION.version == 6
    assert SEARCH_ATTRIBUTES_MIGRATION.name


def test_checksum_is_stable() -> None:
    # Migrations are checksummed so an accidental edit to a released migration is caught;
    # verify the checksum is deterministic and dialect-specific.
    first = SEARCH_ATTRIBUTES_MIGRATION.checksum(Dialect.SQLITE)
    second = SEARCH_ATTRIBUTES_MIGRATION.checksum(Dialect.SQLITE)
    assert first == second
    assert first != SEARCH_ATTRIBUTES_MIGRATION.checksum(Dialect.POSTGRES)


def test_sqlite_ddl_applies_cleanly() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        # lw_search_attributes references lw_executions(run_id); create it first, matching
        # the order migration 1 already establishes.
        from loomwork.store.sql.migrations import MIGRATIONS

        for statement in MIGRATIONS[0].statements(Dialect.SQLITE):
            conn.execute(statement)
        for statement in SEARCH_ATTRIBUTES_MIGRATION.statements(Dialect.SQLITE):
            conn.execute(statement)
        conn.commit()
    finally:
        conn.close()


def test_scalar_unique_index_rejects_duplicate_scalar_rows() -> None:
    from tests.visibility.support import build_sqlite_connection

    conn = build_sqlite_connection()
    try:
        conn.execute(
            "INSERT INTO lw_executions (run_id, workflow_id, workflow_name, status, created_at) "
            "VALUES ('r1', 'w1', 'wf', 'running', 0)"
        )
        conn.execute(
            "INSERT INTO lw_search_attributes (run_id, name, value_type, str_value) "
            "VALUES ('r1', 'tier', 'string', 'gold')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO lw_search_attributes (run_id, name, value_type, str_value) "
                "VALUES ('r1', 'tier', 'string', 'silver')"
            )
    finally:
        conn.close()


def test_keyword_list_allows_multiple_rows_per_name() -> None:
    from tests.visibility.support import build_sqlite_connection

    conn = build_sqlite_connection()
    try:
        conn.execute(
            "INSERT INTO lw_executions (run_id, workflow_id, workflow_name, status, created_at) "
            "VALUES ('r1', 'w1', 'wf', 'running', 0)"
        )
        conn.execute(
            "INSERT INTO lw_search_attributes (run_id, name, value_type, str_value) "
            "VALUES ('r1', 'tags', 'keyword', 'a')"
        )
        # Does not raise: keyword lists intentionally allow several rows per (run_id, name).
        conn.execute(
            "INSERT INTO lw_search_attributes (run_id, name, value_type, str_value) "
            "VALUES ('r1', 'tags', 'keyword', 'b')"
        )
        rows = conn.execute(
            "SELECT str_value FROM lw_search_attributes WHERE run_id = 'r1' AND name = 'tags' "
            "ORDER BY str_value"
        ).fetchall()
        assert [r[0] for r in rows] == ["a", "b"]
    finally:
        conn.close()


def test_value_columns_and_types_cover_every_attribute_type() -> None:
    for attr_type in AttributeType:
        assert attr_type.value in VALUE_COLUMNS
        assert attr_type.value in VALUE_TYPES
