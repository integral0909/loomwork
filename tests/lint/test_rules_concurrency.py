"""LW109: threads, process pools and executors."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestBlockingConcurrency:
    def test_threading_thread_is_flagged(self) -> None:
        # Both the Thread() construction and the chained .start() resolve into
        # "threading" and are each reported.
        findings = lint(
            """
            import threading
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                threading.Thread(target=lambda: None).start()
            """
        )
        assert codes_of(findings) == ["LW109", "LW109"]

    def test_concurrent_futures_and_multiprocessing_are_flagged(self) -> None:
        findings = lint(
            """
            import concurrent.futures
            import multiprocessing
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                concurrent.futures.ThreadPoolExecutor()
                multiprocessing.Process()
            """
        )
        assert codes_of(findings) == ["LW109", "LW109"]

    def test_asyncio_to_thread_is_flagged(self) -> None:
        findings = lint(
            """
            import asyncio
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                await asyncio.to_thread(str, 1)
            """
        )
        assert codes_of(findings) == ["LW109"]

    def test_run_in_executor_is_flagged_even_on_an_opaque_receiver(self) -> None:
        # The loop object is returned by get_event_loop(); the linter cannot (and need
        # not) fully resolve it, only recognize the distinctive method name.
        findings = lint(
            """
            import asyncio
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, str, 1)
            """
        )
        assert codes_of(findings) == ["LW109"]

    def test_awaiting_a_step_is_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.step
            async def do_work() -> int:
                return 1

            @lw.workflow
            async def process() -> None:
                await do_work()
            """
        )
        assert findings == []
