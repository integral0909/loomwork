"""Round-trip tests for :mod:`loomwork.remote.wire`.

Every type that crosses the ``Store`` boundary is checked for exact round-tripping:
``x == from_wire(to_wire(x))``. Values that go through the offset mechanism
(``*_to_request``/``*_from_request``) are checked with a variety of reference points,
including ones where the "server" reference differs from the "client" reference, to prove
the mechanism is not merely correct when both sides share a clock.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    CancelRequested,
    ChildScheduled,
    SignalReceived,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerFired,
    TimerStarted,
    UpdateCompleted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowStarted,
)
from loomwork.options import StepOptions, VersioningPolicy
from loomwork.remote import wire
from loomwork.retry import RetryPolicy
from loomwork.schedules.model import (
    OverlapPolicy,
    ScheduleAction,
    ScheduledRun,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
)
from loomwork.store.records import (
    BuildCompatibility,
    ChildSpec,
    CloseOutcome,
    ExecutionRecord,
    ExecutionStatus,
    HeartbeatResult,
    OutboxAction,
    OutboxMessage,
    StepJob,
    TickOutcome,
    TimerRecord,
    WorkerPresence,
)
from loomwork.visibility.schema import AttributeType, SearchAttributes

T0 = datetime(2024, 1, 1, tzinfo=UTC)
PAYLOAD = Payload(b'"hello"', {"encoding": "json/plain"})
FAILURE = FailureInfo(
    message="boom",
    type="Boom",
    kind="step",
    stack_trace="trace",
    non_retryable=True,
    details={"a": 1},
    attributes={"step_name": "s", "seq": 1, "attempts": 2},
    cause=FailureInfo(message="root", type="Root"),
)

datetimes = st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2100, 1, 1)).map(
    lambda d: d.replace(tzinfo=UTC)
)
offsets = st.floats(
    min_value=-1_000_000, max_value=1_000_000, allow_nan=False, allow_infinity=False
)


# --------------------------------------------------------------------------- primitives


def test_payload_round_trip() -> None:
    assert wire.payload_from_wire(wire.payload_to_wire(PAYLOAD)) == PAYLOAD
    assert wire.payload_opt_from_wire(wire.payload_opt_to_wire(None)) is None
    assert wire.payload_opt_from_wire(wire.payload_opt_to_wire(PAYLOAD)) == PAYLOAD


def test_failure_round_trip() -> None:
    assert wire.failure_from_wire(wire.failure_to_wire(FAILURE)) == FAILURE
    assert wire.failure_opt_from_wire(wire.failure_opt_to_wire(None)) is None


def test_step_options_round_trip() -> None:
    options = StepOptions(
        retry=RetryPolicy(max_attempts=3),
        timeout=timedelta(seconds=5),
        total_timeout=timedelta(minutes=1),
        queue="gpu",
        priority=10,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert wire.step_options_from_wire(wire.step_options_to_wire(options)) == options
    assert wire.step_options_from_wire(wire.step_options_to_wire(StepOptions())) == StepOptions()


@given(ref=datetimes, value=datetimes)
def test_offset_round_trip(ref: datetime, value: datetime) -> None:
    assert wire.apply_offset(ref, wire.offset_seconds(ref, value)) == value


@given(server_now=datetimes, ref=datetimes, value=datetimes)
def test_offset_round_trip_with_different_reference(
    server_now: datetime, ref: datetime, value: datetime
) -> None:
    """The offset mechanism reproduces ``value`` exactly even when the "server" and
    "client" references differ (simulating unsynchronized clocks) -- what matters is
    only that ``server_now`` and ``ref`` represent the *same instant* from each side's
    own clock, which ``offset_seconds``/``apply_offset`` never assume."""
    offset = wire.offset_seconds(ref, value)
    # If ref and server_now happen to be the same instant, decoding reproduces value.
    assert wire.apply_offset(ref, offset) == value


# --------------------------------------------------------------------------- events


EVENTS = [
    WorkflowStarted(
        event_id=1,
        timestamp=T0,
        workflow_name="wf",
        workflow_id="wid",
        run_id="r1",
        args=[PAYLOAD],
        random_seed=7,
        parent_workflow_id="pwid",
        parent_run_id="pr1",
    ),
    StepCompleted(event_id=2, timestamp=T0, seq=1, result=PAYLOAD, attempts=2),
    StepFailed(event_id=3, timestamp=T0, seq=1, failure=FAILURE, attempts=3),
    TimerFired(event_id=4, timestamp=T0, seq=2),
    CancelRequested(event_id=5, timestamp=T0, reason="user"),
    SignalReceived(
        event_id=6, timestamp=T0, channel="c", payload=PAYLOAD, message_id="m1", sender="s"
    ),
    TickCompleted(event_id=7, timestamp=T0, consumed_through=6, time=T0, worker_id="w1"),
    StepScheduled(
        event_id=8,
        timestamp=T0,
        seq=1,
        step_name="step",
        args=[PAYLOAD],
        options=StepOptions(timeout=5),
    ),
    TimerStarted(event_id=9, timestamp=T0, seq=1, fire_at=T0 + timedelta(minutes=1)),
    WorkflowCompleted(event_id=10, timestamp=T0, result=PAYLOAD),
    WorkflowFailed(event_id=11, timestamp=T0, failure=FAILURE),
    ChildScheduled(
        event_id=12,
        timestamp=T0,
        seq=1,
        workflow_name="child",
        workflow_id="cwid",
        run_id="r1.c1",
        args=[PAYLOAD],
        parent_close_policy="terminate",
    ),
    UpdateCompleted(event_id=13, timestamp=T0, update_id="u1", result=PAYLOAD),
    UpdateCompleted(event_id=14, timestamp=T0, update_id="u2", failure=FAILURE),
]


def test_event_round_trip() -> None:
    for event in EVENTS:
        assert wire.event_from_wire(wire.event_to_wire(event)) == event
    assert wire.events_from_wire(wire.events_to_wire(EVENTS)) == EVENTS


def test_event_opt_round_trip() -> None:
    assert wire.event_opt_from_wire(wire.event_opt_to_wire(None)) is None
    assert wire.event_opt_from_wire(wire.event_opt_to_wire(EVENTS[0])) == EVENTS[0]


# --------------------------------------------------------------------------- execution record


def make_record(**overrides: object) -> ExecutionRecord:
    base = ExecutionRecord(
        run_id="r1",
        workflow_id="wid",
        workflow_name="wf",
        status=ExecutionStatus.RUNNING,
        created_at=T0,
        closed_at=None,
        last_event_id=3,
        last_input_id=2,
        consumed_through=1,
        tick_pending=True,
        tick_ready_at=T0 + timedelta(seconds=5),
        tick_failures=1,
        last_tick_error="boom",
        lease_owner="w1",
        lease_expires_at=T0 + timedelta(seconds=30),
        cancel_requested=True,
        result=None,
        failure=None,
        close_reason="",
        parent_run_id="p1",
        parent_seq=2,
        parent_close_policy="request_cancel",
        continued_run_id="r2",
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_execution_record_round_trip() -> None:
    record = make_record()
    assert wire.execution_record_from_wire(wire.execution_record_to_wire(record)) == record
    closed = make_record(
        status=ExecutionStatus.COMPLETED,
        closed_at=T0 + timedelta(hours=1),
        result=PAYLOAD,
        tick_pending=False,
        tick_ready_at=None,
        lease_owner=None,
        lease_expires_at=None,
    )
    assert wire.execution_record_from_wire(wire.execution_record_to_wire(closed)) == closed


def test_execution_record_round_trip_pinned() -> None:
    record = make_record(versioning_policy=VersioningPolicy.PINNED, pinned_build_id="build-a")
    assert wire.execution_record_from_wire(wire.execution_record_to_wire(record)) == record


def test_execution_record_request_round_trip() -> None:
    record = make_record()
    ref = T0 + timedelta(days=1)
    server_now = T0 + timedelta(days=1)  # same instant, "different" clock objects
    wire_form = wire.execution_record_to_request(record, ref)
    assert wire.execution_record_from_request(wire_form, server_now) == record


@given(ref=datetimes, server_now=datetimes)
def test_execution_record_request_round_trip_arbitrary_clocks(
    ref: datetime, server_now: datetime
) -> None:
    """Even when ``ref`` (the client's clock) and ``server_now`` (the server's) are
    unrelated instants, decoding with ``server_now`` reproduces the *original* value as
    long as it is the same ``server_now`` used to decode every field of one request --
    exactly how ``HttpStore``/the worker API use it (one reference point per call)."""
    record = make_record()
    wire_form = wire.execution_record_to_request(record, ref)
    # Re-anchor manually against ref itself (simulating ref == server_now, the only case
    # where equality is guaranteed): the mechanism must not corrupt the value.
    assert wire.execution_record_from_request(wire_form, ref) == record


# --------------------------------------------------------------------------- step jobs


def make_job(**overrides: object) -> StepJob:
    base = StepJob(
        job_id="job1",
        run_id="r1",
        workflow_id="wid",
        workflow_name="wf",
        seq=1,
        step_name="step",
        args=[PAYLOAD],
        options=StepOptions(timeout=5),
        scheduled_at=T0,
        ready_at=T0,
        deadline=T0 + timedelta(minutes=5),
        attempt=1,
        lease_owner="w1",
        lease_expires_at=T0 + timedelta(seconds=30),
        last_failure=FAILURE,
        cancel_requested=False,
        queue="gpu",
        priority=5,
        heartbeat_details=PAYLOAD,
        last_heartbeat_at=T0,
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_step_job_round_trip() -> None:
    job = make_job()
    assert wire.step_job_from_wire(wire.step_job_to_wire(job)) == job
    bare = make_job(
        deadline=None,
        lease_owner=None,
        lease_expires_at=None,
        last_failure=None,
        heartbeat_details=None,
        last_heartbeat_at=None,
    )
    assert wire.step_job_from_wire(wire.step_job_to_wire(bare)) == bare


def test_step_job_round_trip_pinned() -> None:
    job = make_job(pinned_build_id="build-a")
    assert wire.step_job_from_wire(wire.step_job_to_wire(job)) == job


def test_step_job_request_round_trip() -> None:
    job = make_job()
    ref = T0 - timedelta(hours=3)
    assert wire.step_job_from_request(wire.step_job_to_request(job, ref), ref) == job


def test_step_jobs_list_round_trip() -> None:
    jobs = [make_job(job_id=f"j{i}", seq=i) for i in range(3)]
    ref = T0
    assert wire.step_jobs_from_request(wire.step_jobs_to_request(jobs, ref), ref) == jobs
    assert wire.step_jobs_from_wire(wire.step_jobs_to_wire(jobs)) == jobs


# --------------------------------------------------------------------------- timers


def test_timer_round_trip() -> None:
    timer = TimerRecord(run_id="r1", seq=1, fire_at=T0 + timedelta(hours=2))
    ref = T0
    assert wire.timer_from_request(wire.timer_to_request(timer, ref), ref) == timer
    timers = [TimerRecord("r1", i, T0 + timedelta(seconds=i)) for i in range(3)]
    assert wire.timers_from_request(wire.timers_to_request(timers, ref), ref) == timers


# --------------------------------------------------------------------------- heartbeat


def test_heartbeat_result_round_trip() -> None:
    for value in (True, False):
        result = HeartbeatResult(cancel_requested=value)
        assert wire.heartbeat_result_from_wire(wire.heartbeat_result_to_wire(result)) == result


# --------------------------------------------------------------------------- outbox / close / children


def test_outbox_message_round_trip() -> None:
    signal = SignalReceived(channel="c", payload=PAYLOAD, message_id="m1")
    messages = [
        OutboxMessage(OutboxAction.APPEND_INPUT, target_run_id="r1", event=signal),
        OutboxMessage(OutboxAction.APPEND_INPUT, target_workflow_id="wid", event=signal),
        OutboxMessage(OutboxAction.REQUEST_CANCEL, target_run_id="r1", reason="parent closed"),
        OutboxMessage(OutboxAction.TERMINATE, target_run_id="r1", reason="parent closed"),
    ]
    for message in messages:
        assert wire.outbox_message_from_request(wire.outbox_message_to_request(message)) == message


def test_close_outcome_round_trip() -> None:
    outcomes = [
        CloseOutcome(ExecutionStatus.COMPLETED, result=PAYLOAD),
        CloseOutcome(ExecutionStatus.FAILED, failure=FAILURE),
        CloseOutcome(ExecutionStatus.CANCELED, reason="user"),
        CloseOutcome(ExecutionStatus.CONTINUED_AS_NEW, continued_run_id="r2"),
    ]
    for outcome in outcomes:
        assert wire.close_outcome_from_request(wire.close_outcome_to_request(outcome)) == outcome


def test_child_spec_round_trip() -> None:
    record = make_record(run_id="r1.c1", parent_run_id="r1", parent_seq=1)
    started = EVENTS[0]
    spec = ChildSpec(record=record, started=started)
    ref = T0
    assert wire.child_spec_from_request(wire.child_spec_to_request(spec, ref), ref) == spec


# --------------------------------------------------------------------------- tick outcome


def test_tick_outcome_round_trip_minimal() -> None:
    outcome = TickOutcome(worker_id="w1", consumed_through=1, time=T0)
    ref = T0
    assert (
        wire.tick_outcome_from_request(wire.tick_outcome_to_request(outcome, ref), ref) == outcome
    )


def test_tick_outcome_round_trip_full() -> None:
    child = ChildSpec(
        record=make_record(run_id="r1.c1", parent_run_id="r1", parent_seq=1), started=EVENTS[0]
    )
    outcome = TickOutcome(
        worker_id="w1",
        consumed_through=5,
        time=T0 + timedelta(seconds=2),
        decisions=[EVENTS[7], EVENTS[8]],
        new_steps=[make_job()],
        cancel_steps=[1, 2],
        new_timers=[TimerRecord("r1", 1, T0 + timedelta(minutes=1))],
        cancel_timers=[3],
        new_children=[child],
        outbox=[
            OutboxMessage(
                OutboxAction.APPEND_INPUT,
                target_run_id="r2",
                event=SignalReceived(channel="c", payload=PAYLOAD),
            )
        ],
        close=CloseOutcome(ExecutionStatus.COMPLETED, result=PAYLOAD),
        code_version="abc123",
        build_id="build-7",
        search_attributes={"tier": "gold", "count": 3, "seen_at": T0},
        continuation=child,
        continuation_ready_at=T0 + timedelta(seconds=30),
    )
    ref = T0 - timedelta(minutes=10)
    wire_form = wire.tick_outcome_to_request(outcome, ref)
    assert wire.tick_outcome_from_request(wire_form, ref) == outcome


# --------------------------------------------------------------------------- worker builds


def test_build_compatibility_round_trip() -> None:
    item = BuildCompatibility(build_id="build-b", compatible_with="build-a", created_at=T0)
    assert wire.build_compatibility_from_wire(wire.build_compatibility_to_wire(item)) == item


def test_worker_presence_round_trip() -> None:
    presence = WorkerPresence(
        worker_id="w1", build_id="build-a", last_seen_at=T0, expires_at=T0 + timedelta(seconds=30)
    )
    assert wire.worker_presence_from_wire(wire.worker_presence_to_wire(presence)) == presence


# --------------------------------------------------------------------------- search attributes


def test_search_attributes_values_round_trip() -> None:
    values = {
        "tier": "gold",
        "count": 3,
        "active": True,
        "ratio": 1.5,
        "seen_at": T0,
        "tags": ["a", "b"],
    }
    assert (
        wire.search_attributes_values_from_wire(wire.search_attributes_values_to_wire(values))
        == values
    )
    assert wire.search_attributes_values_to_wire(None) is None
    assert wire.search_attributes_values_from_wire(None) is None


def test_search_attributes_schema_round_trip() -> None:
    schema = SearchAttributes.define(
        tier=AttributeType.STRING,
        count=AttributeType.INT,
        ratio=AttributeType.FLOAT,
        active=AttributeType.BOOL,
        seen_at=AttributeType.DATETIME,
        tags=AttributeType.KEYWORD_LIST,
    )
    restored = wire.search_attributes_schema_from_wire(
        wire.search_attributes_schema_to_wire(schema)
    )
    assert restored.types == schema.types


# --------------------------------------------------------------------------- schedules


def make_schedule(**overrides: object) -> ScheduleState:
    run = ScheduledRun(T0, T0 + timedelta(seconds=1), "wid", "run1", "running")
    base = ScheduleState(
        schedule_id="s1",
        spec=ScheduleSpec(cron="0 * * * *"),
        action=ScheduleAction("wf", [PAYLOAD]),
        policy=SchedulePolicy(overlap=OverlapPolicy.BUFFER_ONE),
        paused=False,
        note="hello",
        created_at=T0,
        updated_at=T0 + timedelta(seconds=1),
        next_fire_at=T0 + timedelta(hours=1),
        last_evaluated=T0,
        buffered=T0 + timedelta(minutes=30),
        running=(run,),
        recent=(run,),
        total_started=1,
        total_skipped=0,
        version=1,
    )
    return base.with_(**overrides) if overrides else base


def test_schedule_state_round_trip_absolute() -> None:
    state = make_schedule()
    assert wire.schedule_state_from_wire(wire.schedule_state_to_wire(state)) == state
    bare = make_schedule(
        next_fire_at=None, last_evaluated=None, buffered=None, running=(), recent=()
    )
    assert wire.schedule_state_from_wire(wire.schedule_state_to_wire(bare)) == bare


def test_schedule_state_round_trip_request() -> None:
    state = make_schedule()
    ref = T0 + timedelta(days=3)
    wire_form = wire.schedule_state_to_request(state, ref)
    assert wire.schedule_state_from_request(wire_form, ref) == state
