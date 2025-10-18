"""The linter treats every method of a ``@workflow`` class as workflow code."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


def test_run_and_handlers_of_a_workflow_class_are_checked() -> None:
    findings = lint(
        """
        import random
        import time

        import loomwork as lw


        @lw.workflow
        class Cart:
            @lw.run
            async def run(self) -> float:
                return time.time()

            @lw.signal
            def add(self, item: str) -> None:
                self.lucky = random.random()

            @lw.query
            def count(self) -> int:
                return 0
        """
    )
    lines = sorted(f.location.line for f in findings)
    assert len(findings) == 2
    assert lines == [12, 16]


def test_plain_helper_methods_are_checked_too() -> None:
    findings = lint(
        """
        import uuid

        import loomwork as lw


        @lw.workflow(name="orders")
        class Orders:
            @lw.run
            async def run(self) -> str:
                return self._new_id()

            def _new_id(self) -> str:
                return str(uuid.uuid4())
        """
    )
    assert len(findings) == 1
    assert findings[0].location.line == 14


def test_module_helpers_called_from_methods_are_followed() -> None:
    findings = lint(
        """
        import datetime

        import loomwork as lw


        def stamp() -> str:
            return datetime.datetime.now().isoformat()


        @lw.workflow
        class Stamped:
            @lw.run
            async def run(self) -> str:
                return stamp()
        """
    )
    assert len(findings) == 1
    assert findings[0].call_path == ("run",)


def test_undecorated_classes_are_ignored() -> None:
    findings = lint(
        """
        import time


        class NotAWorkflow:
            async def run(self) -> float:
                return time.time()
        """
    )
    assert codes_of(findings) == []


def test_clean_workflow_class() -> None:
    findings = lint(
        """
        import loomwork as lw


        @lw.workflow
        class Clean:
            @lw.run
            async def run(self) -> str:
                await lw.sleep(5)
                return str(lw.now())
        """
    )
    assert findings == []
