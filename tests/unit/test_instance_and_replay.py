"""Tests for WorkflowInstance and Replayer."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

import loomwork as lw
from loomwork.codec import default_converter
from loomwork.errors import (
    ApplicationError,
    DeterminismViolation,
    NondeterminismError,
    NotInWorkflowError,
    StepError,
    WorkflowTickError,
)
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    CancelRequested,
    Event,
    StepScheduled,
    TimerCanceled,
    TimerStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowTerminated,
)
from loomwork.runtime import (
    CancelTimer,
    CancelWorkflow,
    CompleteWorkflow,
    FailWorkflow,
    HistoryGapError,
    Replayer,
    RequestCancelStep,
    ScheduleStep,
    StartTimer,
    WorkflowInstance,
    replay_history,
)
from loomwork.sdk.definitions import Registry
from tests.helpers import T0, make_instance, step_failed, step_ok, tick, timer_fired

# --------------------------------------------------------------------------- definitions


@lw.step
async def fetch(key: str) -> str:
    return f"value:{key}"


@lw.step(name="compute.total")
async def total(a: int, b: int = 10) -> int:
    return a + b


@lw.workflow
async def sequential(key: str) -> list[str]:
    first = await fetch(key)
    second = await fetch(first)
    return [first, second]


@lw.workflow
async def parallel(n: int) -> list[str]:
    return list(await asyncio.gather(*(fetch(str(i)) for i in range(n))))


@lw.workflow
async def sleepy() -> str:
    before = lw.now()
    await lw.sleep(timedelta(minutes=5))
    return f"{(lw.now() - before).total_seconds()}"


@lw.workflow
async def race() -> str:
    step_task = asyncio.ensure_future(fetch("slow"))
    done, _ = await asyncio.wait({step_task}, timeout=60)
    if done:
        return step_task.result()
    step_task.cancel()
    return "timeout"


@lw.workflow
async def failing() -> None:
    raise ApplicationError("nope", type="Rejected")


@lw.workflow
async def buggy() -> None:
    raise KeyError("programming error")


@lw.workflow(fail_on=[KeyError])
async def buggy_but_final() -> None:
    raise KeyError("expected")


@lw.workflow
async def catches_step_error() -> str:
    try:
        await fetch("x")
    except StepError as exc:
        return f"caught {exc.type}/{exc.__cause__.type if isinstance(exc.__cause__, ApplicationError) else '-'}"
    return "no error"


@lw.workflow
async def uses_randomness() -> list[Any]:
    return [
        lw.random().randint(0, 10**9),
        str(lw.uuid4()),
        lw.info().workflow_id,
        lw.is_replaying(),
    ]


@lw.workflow
async def cancellable() -> str:
    try:
        await lw.sleep(3600)
    except asyncio.CancelledError:
        await fetch("cleanup")
        return "compensated"
    return "slept"


@lw.workflow
async def does_io() -> None:
    await asyncio.to_thread(print, "x")


def run_live(instance: WorkflowInstance, *inputs: Event) -> list[Any]:
    return instance.activate(T0, list(inputs), replaying=False)


# --------------------------------------------------------------------------- instance


class TestInstance:
    def test_sequential_steps(self) -> None:
        inst, started = make_instance(sequential, "k")
        cmds = run_live(inst, started)
        assert cmds == [
            ScheduleStep(seq=1, step_name="fetch", args=default_converter.encode_args(["k"]))
        ]
        cmds = run_live(inst, step_ok(4, 1, "v1"))
        assert [c.step_name for c in cmds] == ["fetch"]
        assert cmds[0].seq == 2
        cmds = run_live(inst, step_ok(7, 2, "v2"))
        assert cmds == [CompleteWorkflow(result=default_converter.encode(["v1", "v2"]))]
        assert inst.closed

    def test_parallel_steps_are_scheduled_in_one_activation(self) -> None:
        inst, started = make_instance(parallel, 3)
        cmds = run_live(inst, started)
        assert [c.seq for c in cmds] == [1, 2, 3]
        # Results arriving out of order still complete deterministically.
        run_live(inst, step_ok(10, 3, "c"))
        cmds = run_live(inst, step_ok(11, 1, "a"), step_ok(12, 2, "b"))
        assert cmds == [CompleteWorkflow(result=default_converter.encode(["a", "b", "c"]))]

    def test_default_arguments_are_recorded(self) -> None:
        @lw.workflow
        async def uses_default() -> int:
            return await total(5)

        inst, started = make_instance(uses_default)
        (cmd,) = run_live(inst, started)
        assert cmd.step_name == "compute.total"
        assert default_converter.decode_args(total.fn, cmd.args) == [5, 10]

    def test_sleep_uses_workflow_time(self) -> None:
        inst, started = make_instance(sleepy)
        (cmd,) = run_live(inst, started)
        assert cmd == StartTimer(seq=1, fire_at=T0 + timedelta(minutes=5))
        later = T0 + timedelta(minutes=7)
        (done,) = inst.activate(later, [timer_fired(5, 1)], replaying=False)
        assert isinstance(done, CompleteWorkflow)
        assert default_converter.decode(done.result) == "420.0"

    def test_cancelled_step_future_emits_request_cancel(self) -> None:
        inst, started = make_instance(race)
        cmds = run_live(inst, started)
        assert [type(c) for c in cmds] == [ScheduleStep, StartTimer]
        cmds = run_live(inst, timer_fired(5, 1))
        assert cmds == [
            RequestCancelStep(seq=1),
            CompleteWorkflow(result=default_converter.encode("timeout")),
        ]

    def test_late_step_result_after_cancel_is_ignored(self) -> None:
        inst, started = make_instance(race)
        run_live(inst, started)
        run_live(inst, timer_fired(5, 1))
        assert inst.closed
        with pytest.raises(WorkflowTickError, match="already produced"):
            run_live(inst, step_ok(8, 1, "late"))

    def test_step_result_cancels_timer(self) -> None:
        inst, started = make_instance(race)
        run_live(inst, started)
        cmds = run_live(inst, step_ok(5, 1, "fast"))
        assert cmds == [
            CancelTimer(seq=1),
            CompleteWorkflow(result=default_converter.encode("fast")),
        ]

    def test_application_error_fails_workflow(self) -> None:
        inst, started = make_instance(failing)
        (cmd,) = run_live(inst, started)
        assert isinstance(cmd, FailWorkflow)
        assert cmd.failure.type == "Rejected"

    def test_unexpected_exception_fails_the_tick(self) -> None:
        inst, started = make_instance(buggy)
        with pytest.raises(WorkflowTickError, match="KeyError"):
            run_live(inst, started)

    def test_fail_on_makes_exception_final(self) -> None:
        inst, started = make_instance(buggy_but_final)
        (cmd,) = run_live(inst, started)
        assert isinstance(cmd, FailWorkflow)
        assert cmd.failure.type == "KeyError"

    def test_step_failure_is_raised_in_workflow(self) -> None:
        inst, started = make_instance(catches_step_error)
        run_live(inst, started)
        failure = FailureInfo(
            message="step failed",
            type="StepError",
            kind="step",
            attributes={"step_name": "fetch", "seq": 1, "attempts": 3},
            cause=FailureInfo(message="nope", type="Upstream"),
        )
        (cmd,) = run_live(inst, step_failed(4, 1, failure))
        assert default_converter.decode(cmd.result) == "caught StepError/Upstream"

    def test_randomness_is_seeded(self) -> None:
        results = []
        for _ in range(2):
            inst, started = make_instance(uses_randomness)
            (cmd,) = run_live(inst, started)
            results.append(default_converter.decode(cmd.result))
        assert results[0] == results[1]
        assert results[0][2] == "wf-id"
        assert results[0][3] is False

    def test_cancellation_allows_compensation(self) -> None:
        inst, started = make_instance(cancellable)
        run_live(inst, started)
        cmds = run_live(inst, CancelRequested(event_id=4, timestamp=T0, reason="user"))
        assert [type(c) for c in cmds] == [CancelTimer, ScheduleStep]
        assert cmds[1].step_name == "fetch"
        (cmd,) = run_live(inst, step_ok(7, 1, "ok"))
        assert default_converter.decode(cmd.result) == "compensated"

    def test_uncaught_cancellation_cancels_workflow(self) -> None:
        inst, started = make_instance(sleepy)
        run_live(inst, started)
        cmds = run_live(inst, CancelRequested(event_id=4, timestamp=T0))
        assert cmds == [CancelTimer(seq=1), CancelWorkflow(reason="canceled")]

    def test_io_fails_the_tick(self) -> None:
        inst, started = make_instance(does_io)
        with pytest.raises(DeterminismViolation):
            run_live(inst, started)

    def test_result_for_unscheduled_step_is_nondeterministic(self) -> None:
        inst, started = make_instance(sequential, "k")
        run_live(inst, started)
        with pytest.raises(NondeterminismError, match="never scheduled"):
            run_live(inst, step_ok(5, 9, "x"))

    def test_close_discards_pending_tasks_quietly(self, caplog: pytest.LogCaptureFixture) -> None:
        import gc

        gc.collect()  # flush instances left behind by earlier tests
        caplog.clear()
        inst, started = make_instance(cancellable)
        run_live(inst, started)
        inst.close()
        del inst
        gc.collect()
        assert "destroyed" not in caplog.text
        assert "never retrieved" not in caplog.text

    def test_pending_introspection(self) -> None:
        inst, started = make_instance(race)
        run_live(inst, started)
        assert inst.pending_step_seqs == [1]
        assert inst.pending_timer_seqs == [1]
        assert inst.main_task is not None


class TestSdkOutsideWorkflow:
    def test_workflow_only_functions_raise(self) -> None:
        for fn in (lw.now, lw.info, lw.random, lw.uuid4, lw.is_replaying):
            with pytest.raises(NotInWorkflowError):
                fn()
        assert not lw.in_workflow()

    async def test_steps_run_directly_outside_workflows(self) -> None:
        assert await fetch("x") == "value:x"
        assert await total(1, b=2) == 3

    async def test_sleep_outside_workflow(self) -> None:
        with pytest.raises(NotInWorkflowError):
            await lw.sleep(1)

    def test_step_info_outside_step(self) -> None:
        with pytest.raises(NotInWorkflowError):
            lw.step_info()


# --------------------------------------------------------------------------- replayer


def _factory(registry: Registry) -> Any:
    def make(started: Any) -> WorkflowInstance:
        return WorkflowInstance(
            registry.workflow(started.workflow_name), started, converter=default_converter
        )

    return make


def _decision(cmd: Any, event_id: int) -> Event:
    import dataclasses

    event: Event = dataclasses.replace(cmd.to_event(), event_id=event_id, timestamp=T0)
    return event


def _sequential_history() -> list[Event]:
    _, started = make_instance(sequential, "k")
    enc = default_converter.encode_args
    return [
        started,
        tick(2, 1),
        StepScheduled(event_id=3, timestamp=T0, seq=1, step_name="fetch", args=enc(["k"])),
        step_ok(4, 1, "v1"),
        tick(5, 4),
        StepScheduled(event_id=6, timestamp=T0, seq=2, step_name="fetch", args=enc(["v1"])),
        step_ok(7, 2, "v2"),
        tick(8, 7),
        WorkflowCompleted(event_id=9, timestamp=T0, result=default_converter.encode(["v1", "v2"])),
    ]


class TestReplayer:
    registry = Registry(
        [sequential, parallel, sleepy, race, cancellable, uses_randomness],
        [fetch, total],
    )

    def test_full_history_replays(self) -> None:
        replayer = replay_history(_sequential_history(), _factory(self.registry))
        assert replayer.is_closed
        assert replayer.replayed_ticks == 3
        assert replayer.instance is not None and replayer.instance.closed

    def test_partial_history_then_live_tick(self) -> None:
        history = _sequential_history()[:7]  # ends with the second step's result
        replayer = replay_history(history, _factory(self.registry))
        assert [e.event_id for e in replayer.pending_inputs] == [7]
        live = replayer.live_tick(T0 + timedelta(seconds=1))
        assert live.consumed_through == 7
        assert live.delivered == 1
        assert isinstance(live.commands[0], CompleteWorkflow)

    def test_incremental_feeding_matches_one_shot(self) -> None:
        history = _sequential_history()
        replayer = Replayer(_factory(self.registry))
        for event in history:
            replayer.feed([event])
        assert replayer.is_closed
        # Re-feeding seen events is a no-op.
        replayer.feed(history)
        assert replayer.last_event_id == 9

    def test_gap_is_detected(self) -> None:
        history = _sequential_history()
        replayer = Replayer(_factory(self.registry))
        with pytest.raises(HistoryGapError):
            replayer.feed([history[0], history[2]])

    def test_changed_step_name_is_nondeterministic(self) -> None:
        history = _sequential_history()
        history[2] = StepScheduled(event_id=3, timestamp=T0, seq=1, step_name="other")
        with pytest.raises(NondeterminismError, match="step='other'.*produced ScheduleStep"):
            replay_history(history, _factory(self.registry))

    def test_missing_command_is_nondeterministic(self) -> None:
        history = _sequential_history()
        history.insert(3, TimerStarted(event_id=4, timestamp=T0, seq=1, fire_at=T0))
        for i, e in enumerate(history[4:], start=5):
            import dataclasses

            history[i - 1] = dataclasses.replace(e, event_id=i)
        with pytest.raises(NondeterminismError, match="did not produce it"):
            replay_history(history, _factory(self.registry))

    def test_extra_command_is_nondeterministic(self) -> None:
        history = _sequential_history()[:2]  # tick recorded no decision
        with pytest.raises(NondeterminismError, match="not in the recorded history"):
            replay_history(history, _factory(self.registry))

    def test_changed_arguments_are_tolerated(self) -> None:
        history = _sequential_history()
        history[2] = StepScheduled(
            event_id=3,
            timestamp=T0,
            seq=1,
            step_name="fetch",
            args=default_converter.encode_args(["different"]),
        )
        assert replay_history(history, _factory(self.registry)).is_closed

    def test_inputs_after_consumed_through_wait_for_next_tick(self) -> None:
        # Both step results arrive, but the tick only consumed the first one.
        inst, started = make_instance(parallel, 2)
        enc = default_converter.encode_args
        history: list[Event] = [
            started,
            tick(2, 1),
            StepScheduled(event_id=3, timestamp=T0, seq=1, step_name="fetch", args=enc(["0"])),
            StepScheduled(event_id=4, timestamp=T0, seq=2, step_name="fetch", args=enc(["1"])),
            step_ok(5, 1, "a"),
            step_ok(6, 2, "b"),
            tick(7, 5),
        ]
        replayer = replay_history(history, _factory(self.registry))
        assert [e.event_id for e in replayer.pending_inputs] == [6]
        live = replayer.live_tick(T0)
        assert live.commands == [CompleteWorkflow(result=default_converter.encode(["a", "b"]))]

    def test_timer_history(self) -> None:
        _, started = make_instance(sleepy)
        history: list[Event] = [
            started,
            tick(2, 1),
            TimerStarted(event_id=3, timestamp=T0, seq=1, fire_at=T0 + timedelta(minutes=5)),
            timer_fired(4, 1),
            tick(5, 4, T0 + timedelta(minutes=5)),
            WorkflowCompleted(event_id=6, timestamp=T0, result=default_converter.encode("300.0")),
        ]
        assert replay_history(history, _factory(self.registry)).is_closed

    def test_cancel_timer_history(self) -> None:
        _, started = make_instance(race)
        history: list[Event] = [
            started,
            tick(2, 1),
            StepScheduled(
                event_id=3,
                timestamp=T0,
                seq=1,
                step_name="fetch",
                args=default_converter.encode_args(["slow"]),
            ),
            TimerStarted(event_id=4, timestamp=T0, seq=1, fire_at=T0 + timedelta(seconds=60)),
            step_ok(5, 1, "fast"),
            tick(6, 5),
            TimerCanceled(event_id=7, timestamp=T0, seq=1),
            WorkflowCompleted(event_id=8, timestamp=T0, result=default_converter.encode("fast")),
        ]
        assert replay_history(history, _factory(self.registry)).is_closed

    def test_randomness_matches_on_replay(self) -> None:
        inst, started = make_instance(uses_randomness)
        (live_cmd,) = inst.activate(T0, [started], replaying=False)
        history = [started, tick(2, 1), _decision(live_cmd, 3)]
        replayer = replay_history(history, _factory(self.registry))
        assert replayer.is_closed

    def test_acknowledged_ticks_are_not_replayed(self) -> None:
        history = _sequential_history()
        replayer = Replayer(_factory(self.registry))
        replayer.feed(history[:4])
        live = replayer.live_tick(T0)
        assert [c.seq for c in live.commands if isinstance(c, ScheduleStep)] == [2]
        replayer.acknowledge(5)
        replayer.feed(history[4:6])  # our own tick and decision
        assert replayer.replayed_ticks == 1
        replayer.feed(history[6:7])
        live = replayer.live_tick(T0)
        assert isinstance(live.commands[0], CompleteWorkflow)

    def test_terminated_history_is_closed(self) -> None:
        _, started = make_instance(sleepy)
        history = [
            started,
            tick(2, 1),
            TimerStarted(event_id=3, timestamp=T0, seq=1, fire_at=T0),
            WorkflowTerminated(event_id=4, timestamp=T0, reason="ops"),
        ]
        replayer = replay_history(history, _factory(self.registry))
        assert replayer.is_closed
        with pytest.raises(WorkflowTickError, match="closed"):
            replayer.live_tick(T0)

    def test_history_without_start(self) -> None:
        replayer = Replayer(_factory(self.registry))
        with pytest.raises(WorkflowTickError, match="does not start"):
            replayer.feed([tick(1, 0)])

    def test_failed_workflow_history(self) -> None:
        registry = Registry([failing])
        _, started = make_instance(failing)
        history = [
            started,
            tick(2, 1),
            WorkflowFailed(
                event_id=3, timestamp=T0, failure=FailureInfo(message="nope", type="Rejected")
            ),
        ]
        assert replay_history(history, _factory(registry)).is_closed

    def test_live_tick_clamps_time(self) -> None:
        history = _sequential_history()[:4]
        replayer = replay_history(history, _factory(self.registry))
        live = replayer.live_tick(T0 - timedelta(hours=1))
        assert live.time == T0

    def test_empty_live_tick(self) -> None:
        history = _sequential_history()[:3]
        replayer = replay_history(history, _factory(self.registry))
        live = replayer.live_tick(T0)
        assert live.is_empty
        assert live.consumed_through == 1
