"""Schema of search attributes: ``lw_search_attributes``.

The table is created by migration 6 in :mod:`loomwork.store.sql.migrations`; this module
re-exports that migration and describes the column layout that
:mod:`loomwork.visibility.sql` compiles queries against, so the two cannot drift apart.

Column layout
    One row per (run_id, name) for scalar attributes, one row per (run_id, name, element)
    for keyword lists (``value_type = 'keyword'``). Exactly one of the typed value columns
    is non-NULL per row, matching ``value_type``. A partial unique index enforces at most
    one row per (run_id, name) for scalar types; keyword lists intentionally allow several.

Known limitation
    Because a keyword list is stored as one row per element, a keyword-list attribute that
    was explicitly set to an empty list is indistinguishable from one that was never set at
    all: both store zero rows. ``attr.<name> IS NULL`` and ``CONTAINS`` therefore treat "set
    to []" and "unset" identically. This matches how callers are expected to populate the
    table (an empty list should simply not be written) and keeps the schema and the query
    compiler simple; revisit only if a real use case needs the distinction.
"""

from __future__ import annotations

from loomwork.store.sql.migrations import MIGRATIONS

#: The migration that creates ``lw_search_attributes`` (defined with the other migrations
#: so the schema has a single source of truth).
SEARCH_ATTRIBUTES_MIGRATION = next(m for m in MIGRATIONS if m.name == "search attributes")

from loomwork.visibility.columns import VALUE_COLUMNS, VALUE_TYPES  # noqa: E402

__all__ = ["SEARCH_ATTRIBUTES_MIGRATION", "VALUE_COLUMNS", "VALUE_TYPES"]
