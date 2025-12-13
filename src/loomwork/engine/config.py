"""Engine configuration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta

from loomwork.options import StepOptions, VersioningPolicy
from loomwork.retry import RetryPolicy
from loomwork.visibility.schema import SearchAttributes


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
        continue_as_new_after_events: history size at which
            :func:`loomwork.continue_as_new_suggested` starts returning ``True``.
        search_attributes: declared search attribute types; queries may only reference
            declared attributes, and values set by clients are validated against it.
        history_warning_events: history size (event count) at which a run logs one
            warning and reports it through :meth:`~loomwork.observability.telemetry.
            Telemetry.history_size_warning`. Purely informational.
        history_limit_events: history size (event count) at which a run is failed
            cleanly with :class:`~loomwork.errors.HistoryLimitExceeded`, recommending
            continue-as-new. Checked before a tick runs, so it is deterministic and
            never leaves a half-written tick (see ``docs/operations/recovery.md``).
        history_warning_bytes, history_limit_bytes: the same two checks, evaluated
            against the store's running total of encoded event bytes rather than event
            count. ``None`` (the default) disables the byte-size checks; the event-count
            ones above are always active.
        default_versioning_policy: :class:`~loomwork.options.VersioningPolicy` a new run
            gets when neither :meth:`~loomwork.engine.engine.Engine.start_workflow`'s
            ``versioning_policy`` argument nor ``workflow_versioning_policy`` says
            otherwise. ``AUTO_UPGRADE`` (today's behaviour) unless changed.
        workflow_versioning_policy: per-workflow-name override of the default, for
            declaring "this workflow type always pins" without every caller of
            ``start_workflow`` repeating it. See ``docs/adr/
            0006-worker-build-versioning.md``.
        pinned_queues: task queues whose step jobs are pinned to a ``PINNED`` run's
            build, alongside its ticks (see :attr:`~loomwork.store.records.StepJob.
            pinned_build_id`). Steps are never replayed, so pinning a queue is a
            deployment choice (workers holding build-specific credentials, binaries or
            drivers), not a correctness requirement; queues not listed here are always
            claimable by any worker, matching today's behaviour.
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
    continue_as_new_after_events: int = 10_000
    search_attributes: SearchAttributes = field(default_factory=lambda: SearchAttributes({}))
    history_warning_events: int = 10_000
    history_limit_events: int = 50_000
    history_warning_bytes: int | None = None
    history_limit_bytes: int | None = None
    default_versioning_policy: VersioningPolicy = VersioningPolicy.AUTO_UPGRADE
    workflow_versioning_policy: Mapping[str, VersioningPolicy] = field(default_factory=dict)
    pinned_queues: frozenset[str] = frozenset()

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
        if self.history_warning_events < 1:
            raise ValueError("history_warning_events must be >= 1")
        if self.history_limit_events < self.history_warning_events:
            raise ValueError("history_limit_events must be >= history_warning_events")
        if self.history_warning_bytes is not None and self.history_warning_bytes < 1:
            raise ValueError("history_warning_bytes must be >= 1")
        if self.history_limit_bytes is not None and (
            self.history_warning_bytes is None
            or self.history_limit_bytes < self.history_warning_bytes
        ):
            raise ValueError(
                "history_limit_bytes must be >= history_warning_bytes, and "
                "history_warning_bytes must be set"
            )

    @property
    def default_step_options(self) -> StepOptions:
        return StepOptions(retry=self.default_step_retry, timeout=self.default_step_timeout)

    def resolve_step_options(self, requested: StepOptions) -> StepOptions:
        """Fill every unset field of ``requested`` from the engine defaults."""
        return self.default_step_options.merged(requested)

    def resolve_versioning_policy(
        self, workflow_name: str, requested: VersioningPolicy | None = None
    ) -> VersioningPolicy:
        """The policy a new run of ``workflow_name`` gets: ``requested`` if given
        (an explicit ``start_workflow`` argument), else the per-workflow override, else
        :attr:`default_versioning_policy`."""
        if requested is not None:
            return requested
        return self.workflow_versioning_policy.get(workflow_name, self.default_versioning_policy)

    def step_lease(self, options: StepOptions) -> timedelta:
        """How long a claimed attempt may go without renewing its lease.

        Steps with a heartbeat timeout renew on every heartbeat, so their lease only
        needs to cover one heartbeat interval; others must cover the whole attempt.
        """
        window = options.heartbeat_timeout or options.timeout or self.default_step_timeout
        return window + self.step_lease_grace
