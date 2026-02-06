"""Round-tripping the result file schema through dicts, JSON, and disk."""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.results import SCHEMA_VERSION, Environment, ResultFile, ScenarioResult


def _sample_result(**overrides: object) -> ScenarioResult:
    fields: dict[str, object] = {
        "scenario": "throughput",
        "description": "Workflows completed per second",
        "store": "memory",
        "case": "simple",
        "params": {"runs": 100, "steps": 1},
        "metric": "throughput",
        "unit": "workflows/s",
        "trials": 5,
        "warmup": 1,
        "n": 5,
        "point": 1234.5,
        "ci_low": 1100.0,
        "ci_high": 1350.0,
        "mean": 1230.0,
        "stdev": 50.0,
        "extra": {"cache_hit_rate": 0.9},
    }
    fields.update(overrides)
    return ScenarioResult(**fields)  # type: ignore[arg-type]


def _sample_file(results: list[ScenarioResult] | None = None) -> ResultFile:
    return ResultFile(
        schema_version=SCHEMA_VERSION,
        environment=Environment.capture(),
        seed=1234,
        quick=True,
        results=results if results is not None else [_sample_result()],
    )


class TestEnvironment:
    def test_capture_fills_every_field(self) -> None:
        env = Environment.capture()
        assert env.python_version
        assert env.platform
        assert env.cpu_count >= 0
        assert env.loomwork_version
        assert env.captured_at

    def test_dict_round_trip(self) -> None:
        env = Environment.capture()
        assert Environment.from_dict(env.to_dict()) == env


class TestScenarioResult:
    def test_dict_round_trip(self) -> None:
        result = _sample_result()
        assert ScenarioResult.from_dict(result.to_dict()) == result

    def test_key_identifies_scenario_store_case(self) -> None:
        result = _sample_result(scenario="tick_latency", store="sqlite", case="cache=on")
        assert result.key == ("tick_latency", "sqlite", "cache=on")

    def test_latency_fields_default_to_none(self) -> None:
        result = _sample_result()
        assert result.p50 is None
        assert result.p95 is None
        assert result.p99 is None


class TestResultFile:
    def test_dict_round_trip(self) -> None:
        result_file = _sample_file(
            [_sample_result(), _sample_result(case="step_heavy", scenario="throughput")]
        )
        assert ResultFile.from_dict(result_file.to_dict()) == result_file

    def test_json_round_trip(self) -> None:
        result_file = _sample_file()
        assert ResultFile.from_json(result_file.to_json()) == result_file

    def test_file_round_trip(self, tmp_path: Path) -> None:
        result_file = _sample_file()
        path = tmp_path / "results.json"
        result_file.save(path)
        assert ResultFile.load(path) == result_file

    def test_rejects_a_future_schema_version(self) -> None:
        result_file = _sample_file()
        payload = result_file.to_dict()
        payload["schema_version"] = SCHEMA_VERSION + 1
        with pytest.raises(ValueError, match="unsupported result schema version"):
            ResultFile.from_dict(payload)
