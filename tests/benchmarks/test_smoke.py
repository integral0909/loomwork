"""One tiny scenario actually executing end to end (scenario -> store -> statistics)
against the backends that need no external service.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.harness import Case, StoreKind, run_case
from benchmarks.scenarios.throughput import ThroughputScenario


def _tiny_case() -> Case:
    return Case("smoke", {"runs": 2, "steps": 1, "payload_bytes": 16}, trials=1, warmup=0)


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
async def test_throughput_smoke(store_kind: StoreKind, tmp_path: Path) -> None:
    scenario = ThroughputScenario()
    result = await run_case(
        scenario,
        store_kind,
        _tiny_case(),
        base_seed=1,
        quick=True,
        work_dir=tmp_path,
        postgres_dsn=None,
    )
    assert result.n == 1
    assert result.point > 0
    assert result.metric == "throughput"
