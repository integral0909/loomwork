"""The result file schema: what a benchmark run produces, and how it is loaded back.

Every scenario/store/case combination becomes one :class:`ScenarioResult`. A whole
``run`` invocation produces one :class:`ResultFile`, with environment metadata attached
so that :func:`benchmarks.report.compare` can refuse to pretend two runs on different
machines are comparable.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import loomwork

#: Bumped when a field is added or removed in a way that breaks :meth:`ResultFile.from_dict`.
SCHEMA_VERSION = 1

Metric = Literal["throughput", "latency"]


def _git_commit() -> str | None:
    try:
        result = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
            ["git", "rev-parse", "--short", "HEAD"],  # noqa: S607 - git is expected on PATH
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


@dataclass(slots=True, frozen=True)
class Environment:
    """Enough about the machine and build to explain why two runs differ."""

    python_version: str
    platform: str
    cpu_count: int
    loomwork_version: str
    git_commit: str | None
    captured_at: str

    @classmethod
    def capture(cls) -> Environment:
        return cls(
            python_version=sys.version.split()[0],
            platform=platform.platform(),
            cpu_count=os.cpu_count() or 0,
            loomwork_version=loomwork.__version__,
            git_commit=_git_commit(),
            captured_at=datetime.now(UTC).isoformat(),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Environment:
        return cls(**data)


@dataclass(slots=True, frozen=True)
class ScenarioResult:
    """One (scenario, store, case) measurement, already reduced to summary statistics.

    ``point`` is the single number :mod:`benchmarks.stats` compares between two result
    files: the median throughput for a throughput metric, the median latency for a
    latency metric. ``ci_low``/``ci_high`` bound it at the confidence level the run
    used (see :mod:`benchmarks.stats`); ``p50``/``p95``/``p99`` and ``mean``/``stdev``
    are reported for humans but are not what regression detection keys on.
    """

    scenario: str
    description: str
    store: str
    case: str
    params: dict[str, Any]
    metric: Metric
    unit: str
    trials: int
    warmup: int
    n: int
    point: float
    ci_low: float
    ci_high: float
    mean: float | None = None
    stdev: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None
    extra: dict[str, float] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str, str]:
        """Identifies this measurement across result files, independent of ``point``."""
        return (self.scenario, self.store, self.case)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScenarioResult:
        return cls(**data)


@dataclass(slots=True, frozen=True)
class ResultFile:
    """A full ``run`` invocation's output: environment plus every scenario result."""

    schema_version: int
    environment: Environment
    seed: int
    quick: bool
    results: list[ScenarioResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "environment": self.environment.to_dict(),
            "seed": self.seed,
            "quick": self.quick,
            "results": [r.to_dict() for r in self.results],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResultFile:
        if data["schema_version"] != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported result schema version {data['schema_version']!r} "
                f"(expected {SCHEMA_VERSION})"
            )
        return cls(
            schema_version=data["schema_version"],
            environment=Environment.from_dict(data["environment"]),
            seed=data["seed"],
            quick=data["quick"],
            results=[ScenarioResult.from_dict(r) for r in data["results"]],
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, text: str) -> ResultFile:
        return cls.from_dict(json.loads(text))

    def save(self, path: Path) -> None:
        path.write_text(self.to_json() + "\n")

    @classmethod
    def load(cls, path: Path) -> ResultFile:
        return cls.from_json(path.read_text())
