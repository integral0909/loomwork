"""Every domain exception maps to a distinct HTTP status and round-trips through
:class:`~loomwork.remote.errors.ProblemDetail` back into the same exception type, so a
caller of :class:`~loomwork.remote.http_store.HttpStore` sees the same exceptions an
embedded store would raise.
"""

from __future__ import annotations

import pytest

from loomwork.errors import (
    LeaseLostError,
    NotFoundError,
    PayloadError,
    RegistrationError,
    ScheduleConflictError,
    ScheduleExistsError,
    WorkflowAlreadyStartedError,
    WorkflowClosedError,
)
from loomwork.remote.errors import ProblemDetail, exception_for, problem_for
from loomwork.remote.wire import WireFormatError
from loomwork.visibility.errors import QuerySyntaxError, QueryTypeError
from loomwork.visibility.lexer import Position


def _round_trip(exc: Exception) -> Exception:
    problem = problem_for(exc)
    wire = problem.to_wire()
    parsed = ProblemDetail.from_wire(problem.status, wire)
    return exception_for(parsed)


@pytest.mark.parametrize(
    ("exc", "status", "code"),
    [
        (NotFoundError("run r1 not found"), 404, "not_found"),
        (LeaseLostError("lease gone"), 409, "lease_lost"),
        (WorkflowClosedError("closed"), 409, "workflow_closed"),
        (PayloadError("bad payload"), 422, "payload_error"),
        (RegistrationError("bad registration"), 422, "registration_error"),
        (ScheduleExistsError("exists"), 409, "schedule_exists"),
        (ScheduleConflictError("conflict"), 409, "schedule_conflict"),
        (ValueError("bad value"), 400, "invalid_request"),
        (WireFormatError("bad wire"), 400, "invalid_request"),
        (TimeoutError("too slow"), 408, "timeout"),
    ],
)
def test_status_and_code(exc: Exception, status: int, code: str) -> None:
    problem = problem_for(exc)
    assert problem.status == status
    assert problem.code == code
    assert problem.detail == str(exc)


def test_workflow_already_started_round_trip() -> None:
    original = WorkflowAlreadyStartedError("order-1", "run-1")
    restored = _round_trip(original)
    assert isinstance(restored, WorkflowAlreadyStartedError)
    assert restored.workflow_id == "order-1"
    assert restored.run_id == "run-1"


@pytest.mark.parametrize(
    "exc",
    [
        NotFoundError("x"),
        LeaseLostError("x"),
        WorkflowClosedError("x"),
        PayloadError("x"),
        ScheduleExistsError("x"),
        ScheduleConflictError("x"),
        ValueError("x"),
        TimeoutError("x"),
    ],
)
def test_round_trip_preserves_type(exc: Exception) -> None:
    restored = _round_trip(exc)
    assert type(restored) is type(exc)
    assert str(restored) == str(exc)


def test_query_error_round_trip_preserves_position_and_source() -> None:
    source = "status = 'bogus'"
    position = Position(line=1, column=10, offset=9)
    original = QueryTypeError("unknown status 'bogus'", source, position)
    restored = _round_trip(original)
    assert isinstance(restored, QueryTypeError)
    assert restored.source == source
    assert restored.position == position
    assert "unknown status" in str(restored)
    assert "^" in restored.format()


def test_query_syntax_error_is_distinct_code() -> None:
    original = QuerySyntaxError("unexpected token", "status = ", Position(1, 10, 9))
    problem = problem_for(original)
    assert problem.code == "query_syntax_error"
    assert problem.status == 400
    restored = exception_for(problem)
    assert isinstance(restored, QuerySyntaxError)


def test_unknown_exception_maps_to_500_and_generic_problem() -> None:
    problem = problem_for(RuntimeError("something broke"))
    assert problem.status == 500
    assert problem.code == "internal_error"
    # Internal error detail is deliberately not exposed to callers.
    assert problem.detail == ""


def test_problem_detail_extra_fields_are_preserved() -> None:
    problem = ProblemDetail(
        "workflow_already_started", "t", "d", 409, {"workflow_id": "w", "run_id": "r"}
    )
    wire = problem.to_wire()
    assert wire["workflow_id"] == "w"
    assert wire["type"] == "https://loomwork.dev/errors/workflow_already_started"
    parsed = ProblemDetail.from_wire(409, wire)
    assert parsed.extra == {"workflow_id": "w", "run_id": "r"}
