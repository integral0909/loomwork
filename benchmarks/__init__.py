"""Loomwork's benchmark suite.

A benchmark here is a :class:`~benchmarks.harness.Scenario`: setup/run/teardown against
a store backend (memory, SQLite, PostgreSQL, or server mode over an in-process HTTP
server), parameterised over runs, steps, fan-out width, payload size, worker count,
concurrency and codec chain. :func:`benchmarks.harness.run_case` turns a scenario's raw
trials into the summary statistics in :mod:`benchmarks.results`; :mod:`benchmarks.cli`
is what ``python -m benchmarks`` runs.

See ``benchmarks/README.md`` for how to run the suite and read its output, and
``docs/performance.md`` for sizing guidance this suite's own numbers back up.
"""

from __future__ import annotations
