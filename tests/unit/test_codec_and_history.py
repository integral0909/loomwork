"""Tests for payload conversion and history (de)serialization."""

from __future__ import annotations

import dataclasses
import enum
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pydantic
import pytest
from hypothesis import given
from hypothesis import strategies as st

from loomwork.codec import DataConverter, Payload, default_converter
from loomwork.codec.payload import ENCODING_BYTES, ENCODING_JSON, ENCODING_NULL
from loomwork.errors import PayloadError
from loomwork.failure import FailureInfo
from loomwork.history import (
    EVENT_TYPES,
    HISTORY_FORMAT_VERSION,
    CancelRequested,
    ChildCompleted,
    ChildFailed,
    ChildScheduled,
    EventCategory,
    ExternalSignalScheduled,
    HistoryFormatError,
    PatchRecorded,
    SignalReceived,
    StepCancelRequested,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerCanceled,
    TimerFired,
    TimerStarted,
    UpdateCompleted,
    UpdateRequested,
    WorkflowCanceled,
    WorkflowCompleted,
    WorkflowContinuedAsNew,
    WorkflowFailed,
    WorkflowStarted,
    WorkflowTerminated,
    event_from_dict,
    event_from_json,
    event_to_dict,
    event_to_json,
    history_from_json,
    history_to_json,
    is_closing,
)
from loomwork.options import StepOptions
from loomwork.retry import RetryPolicy


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Order(pydantic.BaseModel):
    id: str
    total_cents: int
    placed_at: datetime


@dataclasses.dataclass
class Point:
    x: int
    y: int


# --------------------------------------------------------------------------- payloads


class TestPayload:
    def test_dict_roundtrip_with_binary_data(self) -> None:
        payload = Payload(bytes(range(256)), {"encoding": "binary/plain", "k": "v"})
        assert Payload.from_dict(payload.to_dict()) == payload

    def test_with_metadata_does_not_mutate(self) -> None:
        payload = Payload(b"x", {"encoding": "a"})
        updated = payload.with_metadata(encoding="b", extra="1")
        assert payload.encoding == "a"
        assert updated.metadata == {"encoding": "b", "extra": "1"}

    def test_repr_is_truncated(self) -> None:
        assert "..." in repr(Payload(b"x" * 100, {"encoding": ENCODING_JSON}))
        assert len(Payload(b"abc")) == 3


class TestConverter:
    @pytest.mark.parametrize(
        "value",
        [0, -5, 3.25, "text", True, [1, "a", None], {"a": {"b": [1, 2]}}, "", [], {}],
    )
    def test_json_values_roundtrip_untyped(self, value: Any) -> None:
        payload = default_converter.encode(value)
        assert payload.encoding == ENCODING_JSON
        assert default_converter.decode(payload) == value

    def test_none_and_bytes_use_dedicated_encodings(self) -> None:
        assert default_converter.encode(None).encoding == ENCODING_NULL
        assert default_converter.decode(default_converter.encode(None)) is None
        payload = default_converter.encode(b"\x00\xff")
        assert payload.encoding == ENCODING_BYTES
        assert default_converter.decode(payload) == b"\x00\xff"
        assert default_converter.decode(default_converter.encode(bytearray(b"ab")), bytes) == b"ab"

    @pytest.mark.parametrize(
        ("value", "hint"),
        [
            (Order(id="o1", total_cents=500, placed_at=datetime(2024, 5, 1, tzinfo=UTC)), Order),
            (Point(1, 2), Point),
            (Color.BLUE, Color),
            (uuid.UUID(int=42), uuid.UUID),
            (Decimal("12.50"), Decimal),
            (date(2024, 2, 29), date),
            (timedelta(minutes=90), timedelta),
            (datetime(2024, 1, 1, 8, tzinfo=UTC), datetime),
            ((1, "a"), tuple[int, str]),
            ({1, 2, 3}, set[int]),
            ([Point(0, 0)], list[Point]),
            (
                {"a": Order(id="x", total_cents=1, placed_at=datetime(2024, 1, 1, tzinfo=UTC))},
                dict[str, Order],
            ),
        ],
    )
    def test_typed_roundtrip(self, value: Any, hint: Any) -> None:
        assert default_converter.decode(default_converter.encode(value), hint) == value

    def test_decode_validation_error(self) -> None:
        payload = default_converter.encode({"id": "x"})
        with pytest.raises(PayloadError, match="does not match"):
            default_converter.decode(payload, Order)

    def test_encode_unsupported_value(self) -> None:
        with pytest.raises(PayloadError, match="cannot encode"):
            default_converter.encode(object())

    def test_unknown_encoding(self) -> None:
        with pytest.raises(PayloadError, match="unsupported payload encoding"):
            default_converter.decode(Payload(b"", {"encoding": "weird"}))

    def test_invalid_json(self) -> None:
        with pytest.raises(PayloadError, match="not valid JSON"):
            default_converter.decode(Payload(b"{", {"encoding": ENCODING_JSON}))

    def test_decode_args_uses_annotations(self) -> None:
        async def fn(order: Order, count: int, *rest: Any) -> None: ...

        order = Order(id="o", total_cents=1, placed_at=datetime(2024, 1, 1, tzinfo=UTC))
        payloads = default_converter.encode_args([order, 3, {"extra": True}])
        assert default_converter.decode_args(fn, payloads) == [order, 3, {"extra": True}]

    def test_decode_result_uses_return_annotation(self) -> None:
        async def fn() -> Point:
            raise NotImplementedError

        assert default_converter.decode_result(fn, default_converter.encode(Point(3, 4))) == Point(
            3, 4
        )

    def test_unresolvable_annotations_fall_back_to_untyped(self) -> None:
        def fn(x: NotDefinedAnywhere) -> None: ...  # type: ignore[name-defined]  # noqa: F821

        assert DataConverter().decode_args(fn, default_converter.encode_args([{"a": 1}])) == [
            {"a": 1}
        ]

    @given(
        st.recursive(
            st.none() | st.booleans() | st.integers() | st.text(),
            lambda children: st.lists(children) | st.dictionaries(st.text(), children),
            max_leaves=20,
        )
    )
    def test_json_roundtrip_property(self, value: Any) -> None:
        assert default_converter.decode(default_converter.encode(value)) == value


# --------------------------------------------------------------------------- events

T = datetime(2024, 3, 1, 12, tzinfo=UTC)
P = default_converter.encode({"k": 1})
F = FailureInfo(message="bad", type="Bad", cause=FailureInfo(message="root", type="Root"))

SAMPLE_EVENTS = [
    WorkflowStarted(
        workflow_name="wf", workflow_id="w", run_id="r", args=[P, P], random_seed=99, attempt=2
    ),
    StepCompleted(seq=1, result=P, attempts=3),
    StepFailed(seq=2, failure=F, attempts=1),
    TimerFired(seq=4),
    CancelRequested(reason="user"),
    TickCompleted(consumed_through=5, time=T, worker_id="w1"),
    StepScheduled(
        seq=1,
        step_name="charge",
        args=[P],
        options=StepOptions(retry=RetryPolicy(max_attempts=2), timeout=timedelta(seconds=3)),
    ),
    StepCancelRequested(seq=1),
    TimerStarted(seq=2, fire_at=T),
    TimerCanceled(seq=2),
    WorkflowCompleted(result=P),
    WorkflowFailed(failure=F),
    WorkflowCanceled(reason="canceled"),
    WorkflowTerminated(reason="ops"),
    SignalReceived(channel="approvals", payload=P, message_id="m-1", sender="w0"),
    UpdateRequested(update_id="u-1", name="add", args=[P]),
    ChildCompleted(seq=1, result=P),
    ChildFailed(seq=2, failure=F),
    ChildScheduled(
        seq=1,
        workflow_name="child",
        workflow_id="w/child/1",
        run_id="r.c1",
        args=[P],
        parent_close_policy="abandon",
    ),
    UpdateCompleted(update_id="u-1", result=P),
    UpdateCompleted(update_id="u-2", failure=F),
    ExternalSignalScheduled(seq=1, workflow_id="other", channel="c", payload=P, message_id="x"),
    WorkflowContinuedAsNew(new_run_id="r2", workflow_name="wf", args=[P]),
    PatchRecorded(patch_id="sms-receipts"),
]


class TestEvents:
    def test_every_event_kind_has_a_sample(self) -> None:
        assert {e.kind for e in SAMPLE_EVENTS} == set(EVENT_TYPES)

    @pytest.mark.parametrize("event", SAMPLE_EVENTS, ids=lambda e: e.kind)
    def test_dict_and_json_roundtrip(self, event: Any) -> None:
        stamped = dataclasses.replace(event, event_id=12, timestamp=T)
        assert event_from_dict(event_to_dict(stamped)) == stamped
        assert event_from_json(event_to_json(stamped)) == stamped

    def test_categories(self) -> None:
        by_kind = {e.kind: e.category for e in SAMPLE_EVENTS}
        assert by_kind["workflow_started"] is EventCategory.INPUT
        assert by_kind["tick_completed"] is EventCategory.MARKER
        assert by_kind["step_scheduled"] is EventCategory.DECISION
        assert by_kind["workflow_terminated"] is EventCategory.SYSTEM
        assert SAMPLE_EVENTS[1].is_input
        assert SAMPLE_EVENTS[6].is_decision

    def test_closing_events(self) -> None:
        closing = {e.kind for e in SAMPLE_EVENTS if is_closing(e)}
        assert closing == {
            "workflow_completed",
            "workflow_failed",
            "workflow_canceled",
            "workflow_terminated",
            "workflow_continued_as_new",
        }

    def test_unknown_kind(self) -> None:
        with pytest.raises(HistoryFormatError, match="unknown event kind"):
            event_from_dict({"kind": "nope", "id": 1, "ts": T.isoformat()})

    def test_missing_required_field(self) -> None:
        with pytest.raises(HistoryFormatError, match="malformed timer_fired"):
            event_from_dict({"kind": "timer_fired", "id": 1, "ts": T.isoformat()})

    def test_unknown_fields_are_ignored_and_optional_fields_defaulted(self) -> None:
        event = event_from_dict(
            {
                "kind": "step_completed",
                "id": 3,
                "ts": T.isoformat(),
                "seq": 1,
                "result": P.to_dict(),
                "future_field": 1,
            }
        )
        assert isinstance(event, StepCompleted)
        assert event.attempts == 1

    def test_naive_timestamps_are_rejected(self) -> None:
        with pytest.raises(HistoryFormatError, match="naive"):
            event_from_dict({"kind": "timer_fired", "id": 1, "ts": "2024-01-01T00:00:00", "seq": 1})

    def test_duplicate_event_kind_is_rejected(self) -> None:
        from loomwork.history.events import Event

        with pytest.raises(TypeError, match="duplicate event kind"):

            class Clash(Event):
                kind = "timer_fired"

    def test_history_document_roundtrip(self) -> None:
        events = [
            dataclasses.replace(e, event_id=i + 1, timestamp=T) for i, e in enumerate(SAMPLE_EVENTS)
        ]
        text = history_to_json(events)
        assert f'"format_version": {HISTORY_FORMAT_VERSION}' in text
        assert history_from_json(text) == events

    def test_history_document_version_check(self) -> None:
        with pytest.raises(HistoryFormatError, match="unsupported history format"):
            history_from_json('{"format_version": 999, "events": []}')
