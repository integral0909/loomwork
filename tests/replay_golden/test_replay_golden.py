"""Recorded histories must keep decoding and replaying with the current code.

The files in ``histories/`` were recorded once (``python -m tests.replay_golden.record``)
and are committed. They stand in for the histories of runs already in production: if a
change to the event model, the serializer or the runtime stops one of them from
decoding, re-encoding identically, or replaying without divergence, that change would
break real deployments too.

Adding an event kind makes ``test_every_event_kind_is_covered`` fail until a golden
history containing it is recorded, as CONTRIBUTING.md asks.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loomwork.history import history_from_json, history_to_json
from loomwork.history.events import EVENT_TYPES
from loomwork.testing import assert_replays
from tests.replay_golden import corpus

HISTORIES = Path(__file__).parent / "histories"
FILES = sorted(HISTORIES.glob("*.json"))


def test_the_corpus_is_present() -> None:
    assert len(FILES) >= 9


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.stem)
def test_decodes_and_reencodes_identically(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    events = history_from_json(text)
    assert events
    assert history_to_json(events) == text


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.stem)
def test_replays_without_divergence(path: Path) -> None:
    assert_replays(path, *corpus.WORKFLOWS)


def test_every_event_kind_is_covered() -> None:
    recorded = {
        event.kind for path in FILES for event in history_from_json(path.read_text("utf-8"))
    }
    assert set(EVENT_TYPES) - recorded == set()


def test_changed_workflow_code_is_detected() -> None:
    """The replay check has teeth: the recorded order history against a different
    workflow body fails."""
    import loomwork as lw

    @lw.workflow(name="golden.order")
    async def reordered(order_id: str) -> None:
        await lw.sleep(1)  # the recorded history starts with a step, not a timer

    with pytest.raises(AssertionError):
        assert_replays(HISTORIES / "order.json", reordered)
