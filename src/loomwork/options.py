"""Execution options shared by the SDK, the history model and the engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from loomwork.retry import RetryPolicy


def as_timedelta(value: timedelta | float | None) -> timedelta | None:
    """Interpret numbers as seconds; pass ``timedelta`` and ``None`` through."""
    if value is None or isinstance(value, timedelta):
        return value
    return timedelta(seconds=value)


@dataclass(frozen=True, slots=True, init=False)
class StepOptions:
    """How a step is executed.

    Attributes:
        retry: retry policy; ``None`` uses the engine default.
        timeout: maximum duration of a single attempt.
        total_timeout: maximum duration from scheduling until the final outcome,
            across all attempts and backoff delays.
    """

    retry: RetryPolicy | None
    timeout: timedelta | None
    total_timeout: timedelta | None

    def __init__(
        self,
        retry: RetryPolicy | None = None,
        timeout: timedelta | float | None = None,
        total_timeout: timedelta | float | None = None,
    ) -> None:
        object.__setattr__(self, "retry", retry)
        object.__setattr__(self, "timeout", as_timedelta(timeout))
        object.__setattr__(self, "total_timeout", as_timedelta(total_timeout))
        for name in ("timeout", "total_timeout"):
            value = getattr(self, name)
            if value is not None and value <= timedelta(0):
                raise ValueError(f"{name} must be positive")

    def merged(self, override: StepOptions) -> StepOptions:
        """Options where every field set on ``override`` wins."""
        return StepOptions(
            retry=override.retry if override.retry is not None else self.retry,
            timeout=override.timeout if override.timeout is not None else self.timeout,
            total_timeout=(
                override.total_timeout if override.total_timeout is not None else self.total_timeout
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.retry is not None:
            data["retry"] = self.retry.to_dict()
        if self.timeout is not None:
            data["timeout"] = self.timeout.total_seconds()
        if self.total_timeout is not None:
            data["total_timeout"] = self.total_timeout.total_seconds()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StepOptions:
        retry = data.get("retry")
        return cls(
            retry=RetryPolicy.from_dict(retry) if retry is not None else None,
            timeout=as_timedelta(data.get("timeout")),
            total_timeout=as_timedelta(data.get("total_timeout")),
        )
