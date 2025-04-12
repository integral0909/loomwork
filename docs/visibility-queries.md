# Visibility queries

Operators need to find workflow runs: "all failed `orders.process` runs in the last day
where `customer_tier` is gold". `loomwork.visibility` is a small, safe query language for
exactly that. It parses a query string into an AST, type-checks it against a schema of
your search attributes, and either evaluates it in-process against
`ExecutionRecord`/attribute pairs or compiles it to a portable SQL `WHERE`/`ORDER BY`
fragment. Both paths are guaranteed to agree (see "NULL semantics" below, and the
property test in `tests/visibility/test_property_sql_matches_evaluator.py`, which checks
exactly that against thousands of randomly generated queries and fixtures).

```
status = 'failed' AND workflow_name = 'orders.process'
  AND created_at >= now() - interval '1 day'
  AND attr.customer_tier IN ('gold', 'platinum')
  AND NOT (close_reason LIKE 'ops%')
ORDER BY created_at DESC
LIMIT 50
```

## Pipeline

```python
from datetime import UTC, datetime
from loomwork.visibility import (
    AttributeType, SearchAttributes, parse_query, compile_predicate, compile_sql,
)
from loomwork.store.sql.migrations import Dialect

schema = SearchAttributes.define(
    customer_tier=AttributeType.STRING,
    tags=AttributeType.KEYWORD_LIST,
)

query = parse_query("status = 'failed' AND attr.customer_tier = 'gold'")

# In-memory: filter a batch of (record, attrs) you already have.
now = datetime.now(UTC)
predicate = compile_predicate(query, schema, now)
matches = [r for r in records if predicate(r, attrs_by_run[r.run_id])]

# SQL: compile to a WHERE/ORDER BY fragment for lw_executions.
where_sql, params, order_by_sql = compile_sql(
    query, schema, dialect=Dialect.SQLITE, now=now, ts_in=store.ts_in,
)
sql = f"SELECT * FROM lw_executions WHERE {where_sql}"
if order_by_sql:
    sql += f" ORDER BY {order_by_sql}"
sql += " LIMIT ?"
rows = await tx.fetchall(sql, [*params, query.limit or 100])
```

`parse_query` only builds the AST; it does not know your search attribute schema. Both
`compile_predicate` and `compile_sql` call
`loomwork.visibility.typecheck.check_query(query, schema, now)` themselves before doing
anything else, so a query that references an unknown field, mismatches a type, or uses an
operator its field does not support raises `QueryTypeError` before touching a record or a
database. Call `check_query` yourself first if you want to validate a query (for example,
right after an operator types it in) without evaluating or compiling it yet.

`now` is always supplied by the caller, never read from the wall clock. `check_query`
resolves `now()` and interval arithmetic (`now() - interval '1 day'`) into a concrete
timestamp at that moment, so a compiled query is a pure, deterministic function of its
inputs -- running the same `Query` object again later does not change what "1 day ago" means.

## Fields

### Built-in fields

These map directly onto `loomwork.store.records.ExecutionRecord`:

| Field | Type | Nullable |
|---|---|---|
| `run_id` | string | no |
| `workflow_id` | string | no |
| `workflow_name` | string | no |
| `status` | string (one of `running`, `completed`, `failed`, `canceled`, `terminated`, ...) | no |
| `created_at` | timestamp | no |
| `closed_at` | timestamp | yes |
| `tick_failures` | int | no |
| `cancel_requested` | bool | no |
| `close_reason` | string | no |
| `last_tick_error` | string | yes |

A string literal compared against `status` must be one of `ExecutionStatus`'s values;
anything else is a `QueryTypeError` naming the valid ones.

### Search attributes

Search attributes are user-defined key/value pairs attached to a run (`customer_tier =
'gold'`, `region = 'eu-west'`). Define their names and types once, up front:

```python
from loomwork.visibility import SearchAttributes, AttributeType

schema = SearchAttributes.define(
    customer_tier=AttributeType.STRING,
    order_total=AttributeType.FLOAT,
    retry_count=AttributeType.INT,
    is_trial=AttributeType.BOOL,
    activated_at=AttributeType.DATETIME,
    tags=AttributeType.KEYWORD_LIST,
)
```

Names must match `^[a-z][a-z0-9_]{0,62}$` (lowercase, starting with a letter, ASCII word
characters, at most 63 characters); `SearchAttributes.define` raises `SchemaError`
otherwise. In a query, reference an attribute as `attr.<name>`, e.g. `attr.customer_tier`.

Search attributes are always considered nullable -- a run simply may not have a given
attribute set -- regardless of the type declared for the name. `SearchAttributes
.validate_value(name, value)` checks that a Python value fits the declared type (`bool`
for `BOOL`, `datetime` for `DATETIME`, a list of `str` for `KEYWORD_LIST`, and so on); use
it wherever you build the `attrs` mapping passed to the evaluator or write attribute rows,
so a type mismatch is caught at the source rather than surfacing later as a confusing
query result.

## Grammar

Keywords are case-insensitive. Whitespace (including newlines) separates tokens freely.

```ebnf
query        = [ or_expr ] [ order_clause ] [ limit_clause ] ;

or_expr      = and_expr , { "OR" , and_expr } ;
and_expr     = not_expr , { "AND" , not_expr } ;
not_expr     = "NOT" , not_expr
             | "(" , or_expr , ")"
             | comparison ;

comparison   = field , comp_op , scalar
             | field , [ "NOT" ] , "IN" , "(" , scalar_list , ")"
             | field , [ "NOT" ] , "LIKE" , string
             | field , [ "NOT" ] , "BETWEEN" , scalar , "AND" , scalar
             | field , "IS" , [ "NOT" ] , "NULL"
             | field , "CONTAINS" , string ;

comp_op      = "=" | "!=" | "<" | "<=" | ">" | ">=" ;

field        = identifier
             | "attr" , "." , identifier ;

scalar       = string | integer | float | "true" | "false" | "null"
             | "timestamp" , string , [ time_delta ]
             | "now" , "(" , ")" , [ time_delta ] ;

time_delta   = ( "+" | "-" ) , "interval" , string , { ( "+" | "-" ) , "interval" , string } ;

scalar_list  = scalar , { "," , scalar } ;

order_clause = "ORDER" , "BY" , order_term , { "," , order_term } ;
order_term   = field , [ "ASC" | "DESC" ] ;

limit_clause = "LIMIT" , integer ;
```

Lexical notes:

- `string` is single-quoted, with `''` for a literal quote (`'it''s here'`). There is no
  backslash escaping at the string level; `\` is meaningful only inside a `LIKE` pattern
  (see below).
- `integer` / `float` follow the usual decimal / exponent forms (`5`, `-3`, `2.5`, `1e10`).
- `timestamp '...'` bodies are ISO 8601 with an explicit timezone (an offset or a
  trailing `Z`); a timestamp without one is a syntax error, since "what does this
  timestamp mean" should never depend on an assumed timezone.
- `interval '...'` bodies are `<number> <unit>`, where `unit` is one of (any of these
  spellings): `s`/`sec`/`secs`/`second`/`seconds`, `m`/`min`/`mins`/`minute`/`minutes`,
  `h`/`hr`/`hrs`/`hour`/`hours`, `d`/`day`/`days`, `w`/`wk`/`wks`/`week`/`weeks`. A space
  between the number and unit is optional (`'2h'` and `'2 hours'` both work).
- Interval arithmetic (`+`/`-`) only applies to a time-valued expression (`now()` or a
  `timestamp '...'` literal), not to fields or to other intervals; `field + interval
  '1 day'` and `interval '1 day' + interval '1 day'` are both rejected -- the first
  because our grammar's `time_delta` only follows `now()`/`timestamp`, the second by
  `check_query`, which requires the left side of `+`/`-` to resolve to a timestamp.

## Operators

| Operator | Applies to | Notes |
|---|---|---|
| `= != < <= > >=` | any type except keyword lists | `!=` two-valued like the rest: unknown when either side is NULL, not "not equal" |
| `IN (...)` / `NOT IN (...)` | any type except keyword lists | up to 500 values |
| `LIKE` / `NOT LIKE` | strings only | `%` any run of characters, `_` any one character, `\` escapes the next character |
| `IS NULL` / `IS NOT NULL` | any field | the only operator that is never unknown; rejected at type-check time for a built-in field that can never be NULL (e.g. `status IS NULL`) |
| `BETWEEN ... AND ...` / `NOT BETWEEN` | strings, ints, floats, timestamps | not booleans or keyword lists |
| `CONTAINS` | keyword-list attributes only | `attr.tags CONTAINS 'urgent'`; the value is a plain string, not a list |
| `AND` `OR` `NOT` | boolean expressions | precedence `NOT` > `AND` > `OR`, overridable with `(...)` |

`ORDER BY` accepts only built-in fields (not `attr.*`), and only orderable types (string,
int, float, timestamp -- not bool or keyword list). This keeps the in-memory evaluator and
the SQL compiler trivially in agreement: both simply read a fixed `ExecutionRecord`
column. If you need result ordering driven by a search attribute, order by a correlated
built-in field instead (for example `created_at`, which is often a good enough proxy) or
sort the (already filtered) results yourself after fetching them.

## NULL semantics

Loomwork's query language follows standard SQL three-valued logic. A field is NULL when:

- a nullable built-in column has no value (`closed_at` on a still-running execution,
  `last_tick_error` on a run that has never failed a tick), or
- a search attribute was never set on that run.

Every comparison operator (`= != < <= > >= LIKE IN BETWEEN CONTAINS`) evaluates to
**unknown**, not `false`, when either operand is NULL. `IS NULL` / `IS NOT NULL` are the
only operators that are always a definite `true` or `false`. `AND`, `OR` and `NOT`
propagate unknown using Kleene's three-valued logic:

| | `AND` | `OR` |
|---|---|---|
| unknown, false | false | unknown |
| unknown, true | unknown | true |
| unknown, unknown | unknown | unknown |

`NOT unknown` is `unknown`. A top-level `WHERE` keeps a row only when the whole expression
evaluates to `true` -- an `unknown` row is excluded, exactly like a `false` one, which is
exactly how SQL's own `WHERE` behaves. A useful consequence: `NOT (attr.x = 'a')` does
**not** match a run where `attr.x` was never set, because `attr.x = 'a'` is unknown there
and `NOT unknown` is still unknown. Use `attr.x IS NULL OR attr.x != 'a'` if you want runs
where the attribute is unset to count as a match too.

`x IN (a, NULL)` follows the same rule as plain SQL: it is `true` if `x` equals `a`,
otherwise `unknown` (never `false`) because of the `NULL` in the list.

Comparing anything to a literal `NULL` (`attr.x = null`) is always unknown -- just like
`x = NULL` in SQL, this is different from `attr.x IS NULL`, which is a definite check for
"was this ever set".

### Why the SQL compiler gets this for free

Built-in fields compile to a direct column reference, so a NULL column produces SQL's own
native NULL propagation through every operator above -- no special-casing needed on our
part. Search attributes compile to a **scalar subquery**
(`(SELECT sa.<col> FROM lw_search_attributes sa WHERE sa.run_id = lw_executions.run_id AND
sa.name = ? AND sa.value_type = ?)`) rather than an `EXISTS` check: when the attribute was
never set, that subquery itself evaluates to SQL NULL, and every ordinary comparison
built on top of it inherits three-valued NULL handling automatically, identically to the
in-memory evaluator. This relies on there being at most one row per `(run_id, name)` for
scalar attribute types, which the proposed schema (below) enforces with a partial unique
index.

`CONTAINS` is the one exception: a keyword-list attribute is stored as several rows (one
per element), so there is no single value for a scalar subquery to return. It compiles to
a `CASE` that is NULL when the attribute has no rows at all, `1` when a matching element
exists, and `0` otherwise -- reproducing "unset is unknown, set-but-not-matching is false"
without a scalar subquery. See `loomwork/visibility/sql.py` for the exact SQL.

## Limits

Queries are bounded to keep the parser and type checker's cost proportional to what a
human plausibly typed, not to what a script could paste:

| Limit | Value | Constant |
|---|---|---|
| Query text length | 8192 characters | `loomwork.visibility.lexer.MAX_QUERY_LENGTH` |
| Token count | 4000 | `loomwork.visibility.lexer.MAX_TOKENS` |
| Boolean expression nesting depth (parentheses, `NOT` chains) | 64 | `loomwork.visibility.parser.MAX_EXPR_DEPTH` |
| Values in an `IN (...)` list | 500 | `loomwork.visibility.parser.MAX_IN_ITEMS` |
| `LIMIT` value | 10000 | `loomwork.visibility.parser.MAX_LIMIT` |

Exceeding any of these raises `QuerySyntaxError` at parse time.

## Errors

`QuerySyntaxError` (malformed text) and `QueryTypeError` (parses, but does not check out
against the schema) both carry a `position` (`line`, `column`, `offset`) and a
`format()` method that renders the offending source line with a caret, the way a compiler
would:

```
status = 'bogus'
         ^
unknown status 'bogus'; expected one of canceled, completed, continued_as_new, failed, running, terminated
```

## Proposed storage: `lw_search_attributes`

This table does not exist in `loomwork.store.sql.migrations` yet (that module currently
defines migration 1 only: executions, events, step jobs, timers). Adding search attribute
support to the store is:

1. Append `loomwork.visibility.ddl.SEARCH_ATTRIBUTES_MIGRATION` to `MIGRATIONS` in
   `loomwork/store/sql/migrations.py` (renumbering it if other migrations have landed
   first -- it is currently written as version 2).
2. Populate the table wherever executions are created or updated
   (`loomwork/store/sql/base.py` and the in-memory store), validating values with
   `SearchAttributes.validate_value` before writing.
3. Wire `loomwork.visibility.sql.compile_sql` into a `list_executions`-style query method
   that accepts a query string, `SearchAttributes` schema and paging parameters.

Column layout: one row per `(run_id, name)` for a scalar attribute, one row per
`(run_id, name, element)` for a keyword list (`value_type = 'keyword'`). Exactly one of
`str_value` / `int_value` / `float_value` / `bool_value` / `time_value` is populated per
row, matching `value_type`. A partial unique index enforces at most one row per
`(run_id, name)` for scalar types; keyword lists intentionally allow several.

```sql
CREATE TABLE lw_search_attributes (
    run_id      TEXT        NOT NULL REFERENCES lw_executions (run_id),
    name        TEXT        NOT NULL,
    value_type  TEXT        NOT NULL,
    str_value   TEXT,
    int_value   BIGINT,             -- INTEGER in the SQLite dialect
    float_value DOUBLE PRECISION,   -- REAL in the SQLite dialect
    bool_value  BOOLEAN,            -- INTEGER in the SQLite dialect
    time_value  TIMESTAMPTZ         -- INTEGER (microseconds) in the SQLite dialect
);
CREATE INDEX lw_search_attributes_run_id ON lw_search_attributes (run_id);
CREATE INDEX lw_search_attributes_lookup ON lw_search_attributes (name, run_id);
CREATE UNIQUE INDEX lw_search_attributes_scalar_unique
    ON lw_search_attributes (run_id, name) WHERE value_type <> 'keyword';
```

**Known limitation:** because a keyword list is stored as one row per element, a
keyword-list attribute explicitly set to `[]` is indistinguishable from one that was
never set -- both store zero rows. `attr.<name> IS NULL` and `CONTAINS` therefore treat
"set to `[]`" and "unset" identically. Callers should simply not write an empty list.

## Testing your own queries against both backends

`tests/visibility/test_sql_sqlite.py` builds this schema in a real, in-memory SQLite
database (via the `sqlite3` standard library module) and checks every example query's SQL
result against `compile_predicate` applied to the same fixtures.
`tests/visibility/test_property_sql_matches_evaluator.py` does the same with
Hypothesis-generated queries and fixtures, across every operator and NULL combination the
type checker allows -- this is the test that would fail first if the evaluator and the SQL
compiler ever disagreed. `tests/visibility/test_postgres.py` runs a smaller, hand-written
version of the same checks against a real PostgreSQL server when
`LOOMWORK_TEST_POSTGRES_DSN` is set, and is skipped otherwise.
