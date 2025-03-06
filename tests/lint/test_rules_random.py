"""LW102: unseeded randomness."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestRandomness:
    def test_random_module_functions_are_flagged(self) -> None:
        findings = lint(
            """
            import random
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                random.random()
                random.randint(1, 10)
                random.choice([1, 2, 3])
            """
        )
        assert codes_of(findings) == ["LW102"] * 3

    def test_secrets_module_is_flagged(self) -> None:
        findings = lint(
            """
            import secrets
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                secrets.token_hex()
            """
        )
        assert codes_of(findings) == ["LW102"]

    def test_uuid1_and_uuid4_are_flagged(self) -> None:
        findings = lint(
            """
            import uuid
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                uuid.uuid1()
                uuid.uuid4()
            """
        )
        assert codes_of(findings) == ["LW102", "LW102"]

    def test_os_urandom_is_flagged(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                os.urandom(16)
            """
        )
        assert codes_of(findings) == ["LW102"]

    def test_uuid3_and_uuid5_are_not_flagged(self) -> None:
        # Namespace-derived UUIDs are pure functions of their arguments.
        findings = lint(
            """
            import uuid
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                uuid.uuid3(uuid.NAMESPACE_DNS, "example.com")
                uuid.uuid5(uuid.NAMESPACE_DNS, "example.com")
            """
        )
        assert findings == []

    def test_loomwork_random_and_uuid4_are_not_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                lw.random().random()
                lw.uuid4()
            """
        )
        assert findings == []
