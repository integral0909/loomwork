"""The scenario registry: every scenario ``python -m benchmarks`` can run or list."""

from __future__ import annotations

from benchmarks.harness import Scenario
from benchmarks.scenarios.codec import CodecScenario
from benchmarks.scenarios.continuation import ContinuationScenario
from benchmarks.scenarios.dispatch import DispatchLatencyScenario
from benchmarks.scenarios.history_limit import HistoryLimitScenario
from benchmarks.scenarios.messaging import MessagingScenario
from benchmarks.scenarios.replay import ReplayScenario
from benchmarks.scenarios.store_contention import StoreContentionScenario
from benchmarks.scenarios.throughput import ThroughputScenario
from benchmarks.scenarios.tick_latency import TickLatencyScenario
from benchmarks.scenarios.timers import TimerScenario
from benchmarks.scenarios.visibility import VisibilityScenario

#: Registration order is display order for ``list`` and ``run``'s console output.
SCENARIOS: list[Scenario] = [
    ThroughputScenario(),
    DispatchLatencyScenario(),
    TickLatencyScenario(),
    TimerScenario(),
    ReplayScenario(),
    MessagingScenario(),
    ContinuationScenario(),
    HistoryLimitScenario(),
    CodecScenario(),
    StoreContentionScenario(),
    VisibilityScenario(),
]

SCENARIOS_BY_NAME: dict[str, Scenario] = {scenario.name: scenario for scenario in SCENARIOS}

__all__ = ["SCENARIOS", "SCENARIOS_BY_NAME"]
