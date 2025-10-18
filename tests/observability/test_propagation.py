"""W3C Trace Context parsing and formatting."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from loomwork.observability.propagation import (
    MAX_TRACESTATE_MEMBERS,
    TraceContext,
    TraceContextError,
    TraceState,
    derived_span_id,
)

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"
EXAMPLE = f"00-{TRACE_ID}-{SPAN_ID}-01"


# --------------------------------------------------------------------------- traceparent


def test_parses_the_specification_example() -> None:
    context = TraceContext.parse(EXAMPLE)
    assert context.trace_id == TRACE_ID
    assert context.span_id == SPAN_ID
    assert context.sampled
    assert context.traceparent() == EXAMPLE


def test_unsampled_flag() -> None:
    context = TraceContext.parse(f"00-{TRACE_ID}-{SPAN_ID}-00")
    assert not context.sampled
    assert context.traceparent().endswith("-00")


def test_surrounding_whitespace_is_tolerated() -> None:
    assert TraceContext.parse(f"  {EXAMPLE}\t").span_id == SPAN_ID


@pytest.mark.parametrize(
    "header",
    [
        "",
        "garbage",
        f"00-{TRACE_ID}-{SPAN_ID}",  # missing flags
        f"00-{TRACE_ID.upper()}-{SPAN_ID}-01",  # upper case hex
        f"00-{'0' * 32}-{SPAN_ID}-01",  # invalid trace id
        f"00-{TRACE_ID}-{'0' * 16}-01",  # invalid span id
        f"00-{TRACE_ID[:-1]}-{SPAN_ID}-01",  # short trace id
        f"ff-{TRACE_ID}-{SPAN_ID}-01",  # forbidden version
        f"00-{TRACE_ID}-{SPAN_ID}-01-extra",  # version 00 with trailing data
        f"0g-{TRACE_ID}-{SPAN_ID}-01",
    ],
)
def test_rejects_invalid_traceparents(header: str) -> None:
    with pytest.raises(TraceContextError):
        TraceContext.parse(header)


def test_future_versions_are_read_by_their_known_prefix() -> None:
    context = TraceContext.parse(f"cc-{TRACE_ID}-{SPAN_ID}-01-what-the-future-holds")
    assert context.trace_id == TRACE_ID
    # We always emit the version we implement.
    assert context.traceparent() == EXAMPLE
    with pytest.raises(TraceContextError):
        TraceContext.parse(f"cc-{TRACE_ID}-{SPAN_ID}-01.no-dash")


def test_constructor_validates() -> None:
    with pytest.raises(TraceContextError):
        TraceContext("xyz", SPAN_ID)
    with pytest.raises(TraceContextError):
        TraceContext(TRACE_ID, SPAN_ID, flags=256)


def test_new_root_is_random_and_valid() -> None:
    first, second = TraceContext.new_root(), TraceContext.new_root(sampled=False)
    assert first.trace_id != second.trace_id
    assert first.sampled
    assert not second.sampled
    assert TraceContext.parse(first.traceparent()).span_id == first.span_id


def test_child_keeps_trace_flags_and_state() -> None:
    parent = TraceContext.parse(EXAMPLE, "vendor=1")
    child = parent.child("1111111111111111")
    assert child.trace_id == parent.trace_id
    assert child.span_id == "1111111111111111"
    assert child.state == parent.state


# --------------------------------------------------------------------------- headers


def test_header_round_trip() -> None:
    context = TraceContext.parse(EXAMPLE, "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE")
    headers = context.to_headers()
    assert headers == {
        "traceparent": EXAMPLE,
        "tracestate": "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE",
    }
    assert TraceContext.from_headers(headers) == context


def test_from_headers_is_case_insensitive_and_forgiving() -> None:
    assert TraceContext.from_headers({"TraceParent": EXAMPLE}) is not None
    assert TraceContext.from_headers({}) is None
    assert TraceContext.from_headers({"traceparent": "broken"}) is None


def test_invalid_tracestate_is_dropped_not_fatal() -> None:
    context = TraceContext.parse(EXAMPLE, "not a valid member")
    assert context.trace_id == TRACE_ID
    assert not context.state
    assert "tracestate" not in context.to_headers()


# --------------------------------------------------------------------------- tracestate


def test_tracestate_parsing_and_lookup() -> None:
    state = TraceState.parse(["rojo=00f067aa0ba902b7", " congo=t61rcWkgMzE , "])
    assert state.get("congo") == "t61rcWkgMzE"
    assert state.get("missing") is None
    assert state.format() == "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"


def test_multi_tenant_keys() -> None:
    state = TraceState.parse("fw529a3039@dt=1,tenant-1@system=x")
    assert state.get("fw529a3039@dt") == "1"


@pytest.mark.parametrize(
    "value",
    [
        "UPPER=1",  # keys are lower case
        "novalue",
        "key=",  # empty value
        "key=tab\there",  # control characters are not allowed in values
        "key=a,key=b",  # duplicate keys
        "1bad=1",  # simple keys start with a letter
        "k=has=equals",
    ],
)
def test_rejects_invalid_tracestate(value: str) -> None:
    with pytest.raises(TraceContextError):
        TraceState.parse(value)


def test_too_many_members() -> None:
    members = ",".join(f"k{i}=v" for i in range(MAX_TRACESTATE_MEMBERS + 1))
    with pytest.raises(TraceContextError, match="at most"):
        TraceState.parse(members)


def test_with_entry_moves_the_key_to_the_front() -> None:
    state = TraceState.parse("a=1,b=2,c=3")
    updated = state.with_entry("c", "9")
    assert updated.format() == "c=9,a=1,b=2"
    assert state.format() == "a=1,b=2,c=3"
    assert updated.without("a").format() == "c=9,b=2"


def test_with_entry_drops_the_oldest_beyond_the_limit() -> None:
    state = TraceState(tuple((f"k{i}", "v") for i in range(MAX_TRACESTATE_MEMBERS)))
    updated = state.with_entry("new", "v")
    assert len(updated.entries) == MAX_TRACESTATE_MEMBERS
    assert updated.entries[0] == ("new", "v")
    assert updated.get(f"k{MAX_TRACESTATE_MEMBERS - 1}") is None


def test_with_entry_validates() -> None:
    with pytest.raises(TraceContextError):
        TraceState().with_entry("Bad", "v")
    with pytest.raises(TraceContextError):
        TraceState().with_entry("ok", "a,b")


# --------------------------------------------------------------------------- derived ids


def test_derived_span_ids_are_stable_and_distinct() -> None:
    assert derived_span_id("run-1", 3) == derived_span_id("run-1", 3)
    assert derived_span_id("run-1", 3) != derived_span_id("run-1", 4)
    # Parts are delimited: ("ab", "c") and ("a", "bc") differ.
    assert derived_span_id("ab", "c") != derived_span_id("a", "bc")


@given(st.lists(st.one_of(st.text(), st.integers()), min_size=1, max_size=5))
def test_derived_span_ids_are_always_valid(parts: list[str | int]) -> None:
    span = derived_span_id(*parts)
    TraceContext(TRACE_ID, span)


@given(
    st.text(alphabet="0123456789abcdef", min_size=32, max_size=32).filter(lambda s: s.strip("0")),
    st.text(alphabet="0123456789abcdef", min_size=16, max_size=16).filter(lambda s: s.strip("0")),
    st.integers(0, 255),
)
def test_format_parse_round_trip(trace_id: str, span_id: str, flags: int) -> None:
    context = TraceContext(trace_id, span_id, flags)
    assert TraceContext.parse(context.traceparent()) == context
