"""Property: an archived run round-trips through JSON for any history Hypothesis builds.

Complements the end-to-end tests (which archive real runs) by throwing arbitrary event
sequences, execution records and search attributes at :class:`ArchivedRun`'s own
(de)serialization, independent of whether such a history could ever occur in practice.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from loomwork.archive.format import ArchivedRun
from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    Event,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerFired,
    TimerStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowStarted,
)
from loomwork.options import StepOptions
from loomwork.store.records import ExecutionRecord, ExecutionStatus

_TIMESTAMPS = st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2030, 1, 1)).map(
    lambda dt: dt.replace(tzinfo=UTC)
)
_SHORT_TEXT = st.text(min_size=1, max_size=12)
_SEQ = st.integers(min_value=1, max_value=1000)


def _payloads() -> st.SearchStrategy[Payload]:
    return st.builds(
        Payload,
        data=st.binary(max_size=32),
        metadata=st.dictionaries(_SHORT_TEXT, _SHORT_TEXT, max_size=3),
    )


def _failures() -> st.SearchStrategy[FailureInfo]:
    return st.builds(
        FailureInfo,
        message=st.text(max_size=30),
        type=_SHORT_TEXT,
        kind=st.sampled_from(["application", "step", "timeout", "canceled", "unknown"]),
        non_retryable=st.booleans(),
        details=st.none(),
        attributes=st.just({}),
        cause=st.none(),
    )


@st.composite
def _events(draw: st.DrawFn) -> list[Event]:
    count = draw(st.integers(min_value=1, max_value=8))
    events: list[Event] = []
    for event_id in range(1, count + 1):
        timestamp = draw(_TIMESTAMPS)
        kind = draw(
            st.sampled_from(
                [
                    "workflow_started",
                    "step_scheduled",
                    "step_completed",
                    "step_failed",
                    "timer_started",
                    "timer_fired",
                    "tick_completed",
                    "workflow_completed",
                    "workflow_failed",
                ]
            )
        )
        if kind == "workflow_started":
            events.append(
                WorkflowStarted(
                    event_id=event_id,
                    timestamp=timestamp,
                    workflow_name=draw(_SHORT_TEXT),
                    workflow_id=draw(_SHORT_TEXT),
                    run_id=draw(_SHORT_TEXT),
                    args=draw(st.lists(_payloads(), max_size=3)),
                    random_seed=draw(st.integers(min_value=0, max_value=2**31 - 1)),
                )
            )
        elif kind == "step_scheduled":
            events.append(
                StepScheduled(
                    event_id=event_id,
                    timestamp=timestamp,
                    seq=draw(_SEQ),
                    step_name=draw(_SHORT_TEXT),
                    args=draw(st.lists(_payloads(), max_size=3)),
                    options=StepOptions(),
                )
            )
        elif kind == "step_completed":
            events.append(
                StepCompleted(
                    event_id=event_id, timestamp=timestamp, seq=draw(_SEQ), result=draw(_payloads())
                )
            )
        elif kind == "step_failed":
            events.append(
                StepFailed(
                    event_id=event_id,
                    timestamp=timestamp,
                    seq=draw(_SEQ),
                    failure=draw(_failures()),
                )
            )
        elif kind == "timer_started":
            events.append(
                TimerStarted(
                    event_id=event_id,
                    timestamp=timestamp,
                    seq=draw(_SEQ),
                    fire_at=timestamp + timedelta(seconds=draw(st.integers(0, 10_000))),
                )
            )
        elif kind == "timer_fired":
            events.append(TimerFired(event_id=event_id, timestamp=timestamp, seq=draw(_SEQ)))
        elif kind == "tick_completed":
            events.append(
                TickCompleted(
                    event_id=event_id,
                    timestamp=timestamp,
                    consumed_through=draw(st.integers(min_value=0, max_value=event_id)),
                    time=timestamp,
                    code_version=draw(_SHORT_TEXT),
                    build_id=draw(_SHORT_TEXT),
                )
            )
        elif kind == "workflow_completed":
            events.append(
                WorkflowCompleted(event_id=event_id, timestamp=timestamp, result=draw(_payloads()))
            )
        elif kind == "workflow_failed":
            events.append(
                WorkflowFailed(event_id=event_id, timestamp=timestamp, failure=draw(_failures()))
            )
    return events


@st.composite
def _execution_records(draw: st.DrawFn) -> ExecutionRecord:
    status = ExecutionStatus(draw(st.sampled_from([s.value for s in ExecutionStatus])))
    closed_at = draw(_TIMESTAMPS) if status is not ExecutionStatus.RUNNING else None
    return ExecutionRecord(
        run_id=draw(_SHORT_TEXT),
        workflow_id=draw(_SHORT_TEXT),
        workflow_name=draw(_SHORT_TEXT),
        status=status,
        created_at=draw(_TIMESTAMPS),
        closed_at=closed_at,
        last_event_id=draw(st.integers(min_value=0, max_value=1000)),
        last_input_id=draw(st.integers(min_value=0, max_value=1000)),
        consumed_through=draw(st.integers(min_value=0, max_value=1000)),
        tick_failures=draw(st.integers(min_value=0, max_value=10)),
        last_tick_error=draw(st.none() | _SHORT_TEXT),
        cancel_requested=draw(st.booleans()),
        result=draw(st.none() | _payloads()),
        failure=draw(st.none() | _failures()),
        close_reason=draw(st.text(max_size=20)),
        parent_run_id=draw(st.none() | _SHORT_TEXT),
        parent_seq=draw(st.none() | st.integers(min_value=0, max_value=100)),
        continued_run_id=draw(st.none() | _SHORT_TEXT),
    )


_ATTRIBUTE_VALUES = st.one_of(
    st.text(max_size=20),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.booleans(),
)


@st.composite
def _archived_runs(draw: st.DrawFn) -> ArchivedRun:
    return ArchivedRun(
        record=draw(_execution_records()),
        search_attributes=draw(st.dictionaries(_SHORT_TEXT, _ATTRIBUTE_VALUES, max_size=4)),
        history=draw(_events()),
    )


@given(_archived_runs())
@settings(max_examples=60, suppress_health_check=[HealthCheck.too_slow], deadline=None)
def test_archived_run_round_trips_through_json(run: ArchivedRun) -> None:
    text = json.dumps(run.to_dict())
    restored = ArchivedRun.from_dict(json.loads(text))

    assert restored.record == run.record
    assert restored.search_attributes == run.search_attributes
    assert len(restored.history) == len(run.history)
    for original, restored_event in zip(run.history, restored.history, strict=True):
        assert original == restored_event
