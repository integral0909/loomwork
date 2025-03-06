"""The forbidden-call matching table."""

from __future__ import annotations

from loomwork.lint.rules.tables import ForbiddenCall, MatchKind


class TestForbiddenCallMatching:
    def test_exact_match(self) -> None:
        entry = ForbiddenCall("time.sleep", "LW101", MatchKind.EXACT)
        assert entry.matches("time.sleep")
        assert not entry.matches("time.sleep.extra")
        assert not entry.matches("os.system")

    def test_prefix_match(self) -> None:
        entry = ForbiddenCall("socket", "LW106", MatchKind.PREFIX)
        assert entry.matches("socket")
        assert entry.matches("socket.socket")
        assert entry.matches("socket.socket.connect")
        assert not entry.matches("sockets")  # no dot, must not match as a substring

    def test_suffix_match(self) -> None:
        entry = ForbiddenCall("run_in_executor", "LW109", MatchKind.SUFFIX)
        assert entry.matches("run_in_executor")
        assert entry.matches("asyncio.get_event_loop.run_in_executor")
        assert not entry.matches("run_in_executor_but_not_quite")
