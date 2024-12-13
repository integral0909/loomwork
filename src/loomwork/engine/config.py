"""Engine configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from loomwork.options import StepOptions
from loomwork.retry import RetryPolicy


@dataclass(frozen=True, slots=True)
class EngineConfig:
    """Tunables shared by the engine and its workers.

    Attributes:
        tick_lease: how long a workflow worker may hold a run while executing a tick
            before another worker may take over.
        tick_retry: backoff between attempts of a failing tick (for example after a
            nondeterminism error). Attempts are unlimited: a fixed deploy resumes the run.
        default_step_timeout: per-attempt timeout for steps that do not set one.
        default_step_retry: retry policy for steps that do not set one.
        step_lease_grace: added to a step's timeout to form its lease, so a slow but
            live worker is not raced by a second attempt.
        replay_cache_size: live workflow instances each worker keeps between ticks.
        poll_interval: upper bound, in seconds, on how long an idle worker waits before
            checking for work again when no notification arrives.
        timer_batch_size: timers fired per store call.
    """

    tick_lease: timedelta = timedelta(seconds=30)
    tick_retry: RetryPolicy = field(
        default_factory=lambda: RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff=2.0,
            max_interval=timedelta(minutes=5),
            max_attempts=0,
            jitter=0.2,
        )
    )
    default_step_timeout: timedelta = timedelta(minutes=5)
    default_step_retry: RetryPolicy = field(default_factory=RetryPolicy)
    step_lease_grace: timedelta = timedelta(seconds=30)
    replay_cache_size: int = 1000
    poll_interval: float = 1.0
    timer_batch_size: int = 100

    def __post_init__(self) -> None:
        if self.tick_lease <= timedelta(0):
            raise ValueError("tick_lease must be positive")
        if self.default_step_timeout <= timedelta(0):
            raise ValueError("default_step_timeout must be positive")
        if self.replay_cache_size < 0:
            raise ValueError("replay_cache_size must be >= 0")
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if self.timer_batch_size < 1:
            raise ValueError("timer_batch_size must be >= 1")

    @property
    def default_step_options(self) -> StepOptions:
        return StepOptions(retry=self.default_step_retry, timeout=self.default_step_timeout)

    def resolve_step_options(self, requested: StepOptions) -> StepOptions:
        """Fill every unset field of ``requested`` from the engine defaults."""
        return self.default_step_options.merged(requested)

    def step_lease(self, options: StepOptions) -> timedelta:
        timeout = options.timeout or self.default_step_timeout
        return timeout + self.step_lease_grace
