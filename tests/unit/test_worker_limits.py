from __future__ import annotations

import pytest

from loomwork.worker import WorkerOptions
from loomwork.worker.limits import QueueGate, QueueLimit, TokenBucket


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class TestTokenBucket:
    def test_burst_then_refill(self) -> None:
        clock = FakeClock()
        bucket = TokenBucket(2.0, burst=3, clock=clock)
        assert [bucket.try_acquire() for _ in range(4)] == [True, True, True, False]
        assert bucket.seconds_until_token() == pytest.approx(0.5)
        clock.now += 0.5
        assert bucket.try_acquire()
        assert not bucket.peek()

    def test_refill_is_capped_at_burst(self) -> None:
        clock = FakeClock()
        bucket = TokenBucket(10.0, burst=2, clock=clock)
        clock.now += 1000
        assert [bucket.try_acquire() for _ in range(3)] == [True, True, False]

    def test_refund(self) -> None:
        bucket = TokenBucket(1.0, clock=FakeClock())
        assert bucket.try_acquire()
        bucket.refund()
        assert bucket.peek()

    def test_clock_going_backwards_is_harmless(self) -> None:
        clock = FakeClock()
        bucket = TokenBucket(1.0, clock=clock)
        bucket.try_acquire()
        clock.now -= 50
        assert not bucket.try_acquire()

    @pytest.mark.parametrize("rate", [0, -1, float("inf"), float("nan")])
    def test_invalid_rate(self, rate: float) -> None:
        with pytest.raises(ValueError, match="rate"):
            TokenBucket(rate)


class TestQueueGate:
    def test_unrestricted(self) -> None:
        gate = QueueGate(None, {})
        assert gate.open_queues() is None
        assert gate.unrestricted
        assert gate.seconds_until_open() is None

    def test_concurrency_limit(self) -> None:
        gate = QueueGate(["a", "b"], {"a": QueueLimit(concurrency=2)})
        gate.acquire("a")
        assert gate.open_queues() == ("a", "b")
        gate.acquire("a")
        assert gate.open_queues() == ("b",)
        assert gate.running("a") == 2
        gate.release("a")
        assert gate.open_queues() == ("a", "b")
        gate.release("unlimited-queue-is-ignored")

    def test_rate_limit(self) -> None:
        clock = FakeClock()
        gate = QueueGate(["a"], {"a": QueueLimit(rate=1.0)}, clock=clock)
        gate.acquire("a")
        gate.release("a")
        assert gate.open_queues() == ()
        assert gate.seconds_until_open() == pytest.approx(1.0)
        clock.now += 1.0
        assert gate.open_queues() == ("a",)

    def test_rate_overshoot_is_repaid(self) -> None:
        clock = FakeClock()
        gate = QueueGate(["a"], {"a": QueueLimit(rate=1.0)}, clock=clock)
        gate.acquire("a")
        gate.acquire("a")  # a second loop raced past the check
        clock.now += 1.0
        assert gate.open_queues() == ()
        clock.now += 1.0
        assert gate.open_queues() == ("a",)

    def test_limits_for_unpolled_queue_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="does not poll"):
            QueueGate(["a"], {"b": QueueLimit(rate=1)})

    def test_limit_validation(self) -> None:
        with pytest.raises(ValueError, match="rate"):
            QueueLimit(rate=0)
        with pytest.raises(ValueError, match="concurrency"):
            QueueLimit(concurrency=0)


class TestWorkerOptions:
    def test_queue_names_are_validated(self) -> None:
        with pytest.raises(ValueError, match="invalid queue"):
            WorkerOptions(task_queues=("bad queue",))
        with pytest.raises(ValueError, match="must not be empty"):
            WorkerOptions(task_queues=())

    def test_limits_need_explicit_queues(self) -> None:
        with pytest.raises(ValueError, match="explicit task_queues"):
            WorkerOptions(queue_limits={"a": QueueLimit(rate=1)})
