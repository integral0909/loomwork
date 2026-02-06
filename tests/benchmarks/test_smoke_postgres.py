"""The same smoke test as ``test_smoke.py``, against PostgreSQL.

Uses the dedicated ``loomwork_bench`` database (see ``benchmarks/stores.py``), not
``LOOMWORK_TEST_POSTGRES_DSN``'s own database, so it never competes with the rest of the
test suite for that database's contents.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.harness import Case, run_case
from benchmarks.scenarios.throughput import ThroughputScenario
from tests.postgres_support import DSN, requires_postgres

pytestmark = [pytest.mark.postgres, requires_postgres]


async def test_throughput_smoke_postgres(tmp_path: Path) -> None:
    scenario = ThroughputScenario()
    result = await run_case(
        scenario,
        "postgres",
        Case("smoke", {"runs": 2, "steps": 1, "payload_bytes": 16}, trials=1, warmup=0),
        base_seed=1,
        quick=True,
        work_dir=tmp_path,
        postgres_dsn=DSN,
    )
    assert result.n == 1
    assert result.point > 0
