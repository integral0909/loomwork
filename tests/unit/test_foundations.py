"""Tests for ids, clocks, retry policies, options and failures."""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime, timedelta

import pytest

from loomwork import ids
from loomwork.clock import ManualClock, SystemClock, ensure_utc
from loomwork.errors import (
    ApplicationError,
    CanceledFailure,
    StepError,
    TimeoutFailure,
)
from loomwork.failure import MAX_MESSAGE_CHARS, FailureInfo
from loomwork.options import StepOptions, as_timedelta
from loomwork.retry import RetryPolicy

# --------------------------------------------------------------------------- ids


class TestIds:
    def test_ids_are_26_crockford_characters(self) -> None:
        value = ids.new_id()
        assert len(value) == ids.ID_LENGTH
        assert set(value) <= set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")

    def test_ids_are_strictly_increasing_from_one_generator(self) -> None:
        gen = ids.IdGenerator()
        values = [gen.new() for _ in range(5000)]
        assert values == sorted(values)
        assert len(set(values)) == len(values)

    def test_timestamp_roundtrip(self) -> None:
        before = int(datetime.now(UTC).timestamp() * 1000)
        value = ids.new_id()
        after = int(datetime.now(UTC).timestamp() * 1000)
        assert before <= ids.timestamp_ms(value) <= after + 1

    def test_decode_rejects_bad_input(self) -> None:
        with pytest.raises(ValueError, match="expected 26"):
            ids.decode("ABC")
        with pytest.raises(ValueError, match="invalid character"):
            ids.decode("U" * 26)

    def test_decode_is_case_insensitive(self) -> None:
        value = ids.new_id()
        assert ids.decode(value.lower()) == ids.decode(value)


# --------------------------------------------------------------------------- clocks


class TestClocks:
    def test_ensure_utc_rejects_naive(self) -> None:
        with pytest.raises(ValueError, match="naive"):
            ensure_utc(datetime(2024, 1, 1))

    def test_ensure_utc_converts(self) -> None:
        from datetime import timezone

        value = datetime(2024, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))
        assert ensure_utc(value) == datetime(2024, 1, 1, 10, tzinfo=UTC)
        assert ensure_utc(value).tzinfo is UTC

    async def test_manual_clock_advances_and_wakes_sleepers(self) -> None:
        clock = ManualClock(datetime(2024, 1, 1, tzinfo=UTC))
        target = clock.now() + timedelta(seconds=10)
        sleeper = asyncio.create_task(clock.sleep_until(target))
        await asyncio.sleep(0)
        assert not sleeper.done()
        await clock.advance(5)
        await asyncio.sleep(0)
        assert not sleeper.done()
        await clock.advance(timedelta(seconds=5))
        await asyncio.wait_for(sleeper, 1)
        assert clock.version == 2

    async def test_manual_clock_cannot_go_backwards(self) -> None:
        clock = ManualClock()
        with pytest.raises(ValueError, match="backwards"):
            await clock.advance(-1)
        with pytest.raises(ValueError, match="backwards"):
            await clock.set(clock.now() - timedelta(seconds=1))

    async def test_system_clock_sleep_until_past_returns_immediately(self) -> None:
        clock = SystemClock()
        await asyncio.wait_for(clock.sleep_until(clock.now() - timedelta(hours=1)), 0.5)
        assert clock.now().tzinfo is UTC


# --------------------------------------------------------------------------- retry


def _failure(**kwargs: object) -> FailureInfo:
    return FailureInfo(message="x", type=str(kwargs.pop("type", "Boom")), **kwargs)  # type: ignore[arg-type]


class TestRetryPolicy:
    def test_exponential_backoff_is_capped(self) -> None:
        policy = RetryPolicy(initial_interval=1, backoff=3, max_interval=20, jitter=0)
        delays = [policy.delay_for(n).total_seconds() for n in range(1, 6)]
        assert delays == [1, 3, 9, 20, 20]

    def test_jitter_stays_within_bounds(self) -> None:
        policy = RetryPolicy(initial_interval=10, backoff=1, max_interval=10, jitter=0.5)
        rng = random.Random(1)
        values = [policy.delay_for(1, rng).total_seconds() for _ in range(500)]
        assert min(values) >= 5
        assert max(values) <= 15
        assert len(set(values)) > 100

    def test_attempt_limits(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        assert policy.should_retry(1, _failure())
        assert policy.should_retry(2, _failure())
        assert not policy.should_retry(3, _failure())

    def test_unlimited_attempts(self) -> None:
        assert RetryPolicy(max_attempts=0).should_retry(10_000, _failure())

    def test_non_retryable_failures(self) -> None:
        policy = RetryPolicy(non_retryable_types=frozenset({"CardDeclined"}))
        assert not policy.should_retry(1, _failure(type="CardDeclined"))
        assert not policy.should_retry(1, _failure(non_retryable=True))

    def test_no_retry(self) -> None:
        assert not RetryPolicy.no_retry().should_retry(1, _failure())

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"initial_interval": 0}, "initial_interval"),
            ({"initial_interval": 10, "max_interval": 1}, "max_interval"),
            ({"backoff": 0.5}, "backoff"),
            ({"max_attempts": -1}, "max_attempts"),
            ({"jitter": 2}, "jitter"),
        ],
    )
    def test_validation(self, kwargs: dict[str, float], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            RetryPolicy(**kwargs)  # type: ignore[arg-type]

    def test_delay_for_rejects_attempt_zero(self) -> None:
        with pytest.raises(ValueError, match="start at 1"):
            RetryPolicy().delay_for(0)

    def test_dict_roundtrip(self) -> None:
        policy = RetryPolicy(
            initial_interval=0.5,
            backoff=1.5,
            max_interval=30,
            max_attempts=7,
            jitter=0.3,
            non_retryable_types=frozenset({"A", "B"}),
        )
        assert RetryPolicy.from_dict(policy.to_dict()) == policy


# --------------------------------------------------------------------------- options


class TestStepOptions:
    def test_numbers_are_seconds(self) -> None:
        options = StepOptions(timeout=1.5, total_timeout=60)
        assert options.timeout == timedelta(seconds=1.5)
        assert options.total_timeout == timedelta(minutes=1)

    def test_rejects_non_positive(self) -> None:
        with pytest.raises(ValueError, match="timeout"):
            StepOptions(timeout=timedelta(0))

    def test_merge_prefers_override(self) -> None:
        base = StepOptions(retry=RetryPolicy(max_attempts=2), timeout=timedelta(seconds=5))
        merged = base.merged(StepOptions(timeout=timedelta(seconds=9)))
        assert merged.retry == RetryPolicy(max_attempts=2)
        assert merged.timeout == timedelta(seconds=9)
        assert merged.total_timeout is None

    def test_dict_roundtrip(self) -> None:
        options = StepOptions(
            retry=RetryPolicy(max_attempts=4),
            timeout=timedelta(seconds=3),
            total_timeout=timedelta(minutes=2),
        )
        assert StepOptions.from_dict(options.to_dict()) == options
        assert StepOptions.from_dict({}) == StepOptions()

    def test_as_timedelta(self) -> None:
        assert as_timedelta(None) is None
        assert as_timedelta(2) == timedelta(seconds=2)
        assert as_timedelta(timedelta(days=1)) == timedelta(days=1)


# --------------------------------------------------------------------------- failures


def _raise(exc: BaseException) -> BaseException:
    try:
        raise exc
    except BaseException as caught:
        return caught


class TestFailureInfo:
    def test_application_error_roundtrip(self) -> None:
        exc = _raise(
            ApplicationError(
                "declined", type="CardDeclined", non_retryable=True, details={"code": 51}
            )
        )
        info = FailureInfo.from_exception(exc)
        assert info.type == "CardDeclined"
        assert info.kind == "application"
        assert info.non_retryable
        assert info.details == {"code": 51}
        assert "ApplicationError" in info.stack_trace

        rebuilt = info.to_exception()
        assert isinstance(rebuilt, ApplicationError)
        assert rebuilt.type == "CardDeclined"
        assert rebuilt.details == {"code": 51}
        assert rebuilt.failure is info

    def test_arbitrary_exception_becomes_application_error_with_qualified_type(self) -> None:
        info = FailureInfo.from_exception(_raise(KeyError("missing")))
        assert info.type == "KeyError"
        assert info.kind == "unknown"
        rebuilt = info.to_exception()
        assert isinstance(rebuilt, ApplicationError)
        assert rebuilt.type == "KeyError"

    def test_module_qualified_type_name(self) -> None:
        class LocalError(Exception):
            pass

        info = FailureInfo.from_exception(_raise(LocalError("x")))
        assert info.type.endswith(
            "TestFailureInfo.test_module_qualified_type_name.<locals>.LocalError"
        )
        assert info.type.startswith("tests.unit.test_foundations")

    def test_cause_chain_is_captured_and_rebuilt(self) -> None:
        try:
            try:
                raise ValueError("inner")
            except ValueError as inner:
                raise ApplicationError("outer") from inner
        except ApplicationError as exc:
            info = FailureInfo.from_exception(exc)
        assert [f.message for f in info.chain()] == ["outer", "inner"]
        assert info.root_cause().type == "ValueError"
        rebuilt = info.to_exception()
        assert isinstance(rebuilt.__cause__, ApplicationError)
        assert rebuilt.__cause__.message == "inner"

    def test_implicit_context_is_captured_unless_suppressed(self) -> None:
        try:
            try:
                raise ValueError("first")
            except ValueError:
                raise RuntimeError("second")  # noqa: B904
        except RuntimeError as exc:
            info = FailureInfo.from_exception(exc)
        assert info.cause is not None
        assert info.cause.message == "first"

        try:
            try:
                raise ValueError("first")
            except ValueError:
                raise RuntimeError("second") from None
        except RuntimeError as exc:
            assert FailureInfo.from_exception(exc).cause is None

    def test_step_error_attributes(self) -> None:
        exc = StepError("failed", step_name="charge", seq=3, attempts=5)
        info = FailureInfo.from_exception(exc)
        assert info.kind == "step"
        assert info.attributes == {"step_name": "charge", "seq": 3, "attempts": 5}
        rebuilt = info.to_exception()
        assert isinstance(rebuilt, StepError)
        assert (rebuilt.step_name, rebuilt.seq, rebuilt.attempts) == ("charge", 3, 5)

    @pytest.mark.parametrize(
        ("cls", "kind"), [(TimeoutFailure, "timeout"), (CanceledFailure, "canceled")]
    )
    def test_kinds(self, cls: type[ApplicationError], kind: str) -> None:
        info = FailureInfo.from_exception(cls("x"))
        assert info.kind == kind
        assert isinstance(info.to_exception(), cls)

    def test_dict_roundtrip(self) -> None:
        info = FailureInfo(
            message="m",
            type="T",
            kind="step",
            stack_trace="trace",
            non_retryable=True,
            details=[1, 2],
            attributes={"seq": 1},
            cause=FailureInfo(message="c", type="C"),
        )
        assert FailureInfo.from_dict(info.to_dict()) == info

    def test_minimal_dict_omits_defaults(self) -> None:
        assert FailureInfo(message="m", type="T").to_dict() == {
            "message": "m",
            "type": "T",
            "kind": "application",
        }

    def test_long_messages_are_truncated(self) -> None:
        info = FailureInfo.from_exception(ValueError("x" * (MAX_MESSAGE_CHARS * 2)))
        assert len(info.message) < MAX_MESSAGE_CHARS + 50
        assert info.message.endswith("(truncated)")

    def test_deep_cause_chains_are_bounded(self) -> None:
        exc: BaseException = ValueError("0")
        for depth in range(1, 60):
            new = ValueError(str(depth))
            new.__cause__ = exc
            exc = new
        assert len(FailureInfo.from_exception(exc).chain()) <= 17
