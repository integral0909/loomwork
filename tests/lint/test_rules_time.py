"""LW101: wall-clock time."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestWallClockTime:
    def test_time_time_is_flagged(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.time()
            """
        )
        assert codes_of(findings) == ["LW101"]
        assert "time.time" in findings[0].message

    def test_time_sleep_time_monotonic_and_perf_counter_are_flagged(self) -> None:
        findings = lint(
            """
            import time
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                time.sleep(1)
                time.monotonic()
                time.perf_counter()
            """
        )
        assert codes_of(findings) == ["LW101", "LW101", "LW101"]

    def test_datetime_now_utcnow_today_and_date_today_are_flagged(self) -> None:
        findings = lint(
            """
            import datetime
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                datetime.datetime.now()
                datetime.datetime.utcnow()
                datetime.datetime.today()
                datetime.date.today()
            """
        )
        assert codes_of(findings) == ["LW101"] * 4

    def test_datetime_now_flagged_via_from_import(self) -> None:
        findings = lint(
            """
            from datetime import datetime
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                datetime.now()
            """
        )
        assert codes_of(findings) == ["LW101"]

    def test_loomwork_now_and_sleep_are_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                await lw.now()
                await lw.sleep(1)
            """
        )
        assert findings == []

    def test_asyncio_get_event_loop_time_is_not_flagged(self) -> None:
        # The deterministic event loop's own clock is fine; only the wall clock is not.
        findings = lint(
            """
            import asyncio
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                asyncio.get_event_loop().time()
            """
        )
        assert findings == []
