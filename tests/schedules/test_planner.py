"""Unit tests for schedule specs and the planner (no store involved)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    ScheduledRun,
    ScheduleError,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    scheduled_workflow_id,
    validate_schedule_id,
)
from loomwork.schedules.planner import (
    plan,
    plan_backfill,
    plan_trigger,
    record_starts,
    refresh_running,
)
from loomwork.store.records import ExecutionStatus

T0 = datetime(2024, 1, 1, tzinfo=UTC)
HOURLY = ScheduleSpec(cron="0 * * * *")


def state(
    spec: ScheduleSpec = HOURLY,
    *,
    overlap: OverlapPolicy = OverlapPolicy.SKIP,
    window: float = 60,
    running: tuple[ScheduledRun, ...] = (),
    **kwargs: object,
) -> ScheduleState:
    return ScheduleState(
        schedule_id="nightly",
        spec=spec,
        action=ScheduleAction("wf"),
        policy=SchedulePolicy(overlap, window),
        created_at=T0,
        last_evaluated=T0,
        running=running,
        **kwargs,  # type: ignore[arg-type]
    )


RUN = ScheduledRun(T0, T0, "nightly@x", "run-1")


class TestSpec:
    def test_needs_exactly_one_of_cron_and_interval(self) -> None:
        with pytest.raises(ScheduleError, match="exactly one"):
            ScheduleSpec()
        with pytest.raises(ScheduleError, match="exactly one"):
            ScheduleSpec(cron="* * * * *", interval=60)

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"interval": 0.5}, "at least one second"),
            ({"interval": 60, "offset": 60}, "offset"),
            ({"interval": 60, "jitter": -1}, "jitter"),
            ({"cron": "bad"}, "fields"),
            ({"cron": "* * * * *", "start_at": T0, "end_at": T0}, "end_at"),
            ({"cron": "* * * * *", "start_at": datetime(2024, 1, 1)}, "timezone-aware"),
        ],
    )
    def test_validation(self, kwargs: dict[str, object], message: str) -> None:
        with pytest.raises(ScheduleError, match=message):
            ScheduleSpec(**kwargs)  # type: ignore[arg-type]

    def test_interval_alignment(self) -> None:
        spec = ScheduleSpec(interval=timedelta(hours=1), offset=timedelta(minutes=15))
        assert spec.next_after(T0) == T0 + timedelta(minutes=15)
        assert spec.next_after(T0 + timedelta(minutes=15)) == T0 + timedelta(hours=1, minutes=15)

    def test_bounds(self) -> None:
        spec = ScheduleSpec(
            cron="0 0 * * *", start_at=T0 + timedelta(days=2), end_at=T0 + timedelta(days=3)
        )
        assert spec.next_after(T0) == T0 + timedelta(days=2)
        assert spec.next_after(T0 + timedelta(days=2)) == T0 + timedelta(days=3)
        assert spec.next_after(T0 + timedelta(days=3)) is None

    def test_fire_times(self) -> None:
        assert HOURLY.fire_times(T0, T0 + timedelta(hours=3)) == [
            T0 + timedelta(hours=h) for h in (1, 2, 3)
        ]
        assert len(HOURLY.fire_times(T0, T0 + timedelta(days=100), limit=5)) == 5

    def test_jitter_is_stable_and_bounded(self) -> None:
        spec = ScheduleSpec(cron="0 * * * *", jitter=60)
        a = spec.jitter_for("s", T0)
        assert a == spec.jitter_for("s", T0)
        assert timedelta(0) <= a < timedelta(seconds=60)
        assert (
            spec.jitter_for("other", T0) != a or spec.jitter_for("s", T0 + timedelta(hours=1)) != a
        )

    def test_roundtrip_and_describe(self) -> None:
        for spec in (
            ScheduleSpec(cron="0 9 * * 1-5", timezone="Europe/Berlin", jitter=5),
            ScheduleSpec(interval=90, offset=30, start_at=T0, end_at=T0 + timedelta(days=1)),
        ):
            assert ScheduleSpec.from_dict(spec.to_dict()) == spec
        assert (
            "Europe/Berlin" in ScheduleSpec(cron="0 9 * * *", timezone="Europe/Berlin").describe()
        )
        assert "every" in ScheduleSpec(interval=60).describe()

    def test_state_roundtrip(self) -> None:
        s = state(running=(RUN,), buffered=T0, paused=True, note="n", total_started=3)
        assert ScheduleState.from_dict(s.to_dict()) == s

    def test_ids(self) -> None:
        assert validate_schedule_id("daily-report.v2") == "daily-report.v2"
        with pytest.raises(ScheduleError):
            validate_schedule_id("has space")
        assert scheduled_workflow_id("s", T0) == "s@2024-01-01T00:00:00Z"


class TestPlanner:
    def test_nothing_due(self) -> None:
        result = plan(state(), T0 + timedelta(minutes=30), {})
        assert result.starts == []
        assert result.state.next_fire_at == T0 + timedelta(hours=1)

    def test_starts_when_due(self) -> None:
        now = T0 + timedelta(hours=1, seconds=2)
        result = plan(state(), now, {})
        assert [s.nominal_time for s in result.starts] == [T0 + timedelta(hours=1)]
        assert result.starts[0].workflow_id == "nightly@2024-01-01T01:00:00Z"
        assert result.state.last_evaluated == now
        assert result.state.next_fire_at == T0 + timedelta(hours=2)

    def test_outage_beyond_catchup_window_is_skipped(self) -> None:
        now = T0 + timedelta(hours=5, seconds=90)
        result = plan(state(window=60), now, {})
        assert result.starts == []
        assert result.skipped == 5
        assert result.state.total_skipped == 5

    def test_only_latest_in_window_starts(self) -> None:
        now = T0 + timedelta(hours=3)
        result = plan(state(window=timedelta(hours=10).total_seconds()), now, {})
        assert [s.nominal_time for s in result.starts] == [now]
        assert result.skipped == 2

    def test_allow_all_starts_each_fire_time(self) -> None:
        now = T0 + timedelta(hours=3)
        s = state(
            overlap=OverlapPolicy.ALLOW_ALL,
            window=timedelta(hours=10).total_seconds(),
            running=(RUN,),
        )
        result = plan(s, now, {"run-1": ExecutionStatus.RUNNING})
        assert len(result.starts) == 3

    def test_skip_when_running(self) -> None:
        now = T0 + timedelta(hours=1)
        result = plan(state(running=(RUN,)), now, {"run-1": ExecutionStatus.RUNNING})
        assert result.starts == []
        assert result.skipped == 1

    def test_closed_runs_move_to_recent(self) -> None:
        now = T0 + timedelta(hours=1)
        result = plan(state(running=(RUN,)), now, {"run-1": ExecutionStatus.COMPLETED})
        assert len(result.starts) == 1
        assert result.state.running == ()
        assert result.state.recent[-1].status == "completed"

    def test_unknown_runs_are_treated_as_closed(self) -> None:
        refreshed, failed = refresh_running(state(running=(RUN,)), {})
        assert refreshed.running == ()
        assert refreshed.recent[-1].status == "unknown"
        assert not failed

    def test_buffer_one(self) -> None:
        running = {"run-1": ExecutionStatus.RUNNING}
        s = state(overlap=OverlapPolicy.BUFFER_ONE, running=(RUN,))
        first = plan(s, T0 + timedelta(hours=1), running)
        assert first.starts == []
        assert first.state.buffered == T0 + timedelta(hours=1)
        second = plan(first.state, T0 + timedelta(hours=2), running)
        assert second.state.buffered == T0 + timedelta(hours=2)
        assert second.skipped == 1
        # The running run closes: the buffered fire time starts at once.
        third = plan(
            second.state, T0 + timedelta(hours=2, minutes=10), {"run-1": ExecutionStatus.COMPLETED}
        )
        assert [s.nominal_time for s in third.starts] == [T0 + timedelta(hours=2)]
        assert third.state.buffered is None

    @pytest.mark.parametrize("overlap", [OverlapPolicy.CANCEL_OTHER, OverlapPolicy.TERMINATE_OTHER])
    def test_cancel_or_terminate_other(self, overlap: OverlapPolicy) -> None:
        result = plan(
            state(overlap=overlap, running=(RUN,)),
            T0 + timedelta(hours=1),
            {"run-1": ExecutionStatus.RUNNING},
        )
        assert len(result.starts) == 1
        if overlap is OverlapPolicy.CANCEL_OTHER:
            assert result.cancel_run_ids == ["run-1"]
            assert result.state.running == (RUN,)
        else:
            assert result.terminate_run_ids == ["run-1"]
            assert result.state.running == ()

    def test_paused_schedule_skips_and_advances(self) -> None:
        result = plan(state(paused=True), T0 + timedelta(hours=3), {})
        assert result.starts == []
        assert result.skipped == 3
        assert result.state.last_evaluated == T0 + timedelta(hours=3)

    def test_pause_on_failure(self) -> None:
        s = state(running=(RUN,)).with_(policy=SchedulePolicy(pause_on_failure=True))
        result = plan(s, T0 + timedelta(hours=1), {"run-1": ExecutionStatus.FAILED})
        assert result.paused_now
        assert result.state.paused
        assert result.starts == []

    def test_ended_schedule_has_no_next_fire(self) -> None:
        spec = ScheduleSpec(cron="0 * * * *", end_at=T0 + timedelta(hours=1))
        result = plan(state(spec), T0 + timedelta(hours=1), {})
        assert len(result.starts) == 1
        assert result.state.next_fire_at is None

    def test_trigger_and_backfill(self) -> None:
        now = T0 + timedelta(minutes=7)
        assert [s.nominal_time for s in plan_trigger(state(), now).starts] == [now]
        backfill = plan_backfill(state(), T0, T0 + timedelta(hours=4))
        assert len(backfill.starts) == 4
        assert len({s.workflow_id for s in backfill.starts}) == 4

    def test_record_starts_caps_recent(self) -> None:
        runs = [ScheduledRun(T0, T0, f"w{i}", f"r{i}") for i in range(15)]
        updated = record_starts(state(), runs, T0)
        assert len(updated.running) == 15
        assert len(updated.recent) == 10
        assert updated.total_started == 15
