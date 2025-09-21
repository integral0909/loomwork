"""History visualization: pure functions over a hand-built, deterministic history.

The golden outputs below were captured from a fixed history covering one entry per
lane (step, timer, child, wait) so a change to the rendering is a visible diff here,
not a surprise in production.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from loomwork.codec.payload import ENCODING_NULL, Payload
from loomwork.debug.graph import (
    TimelineEntry,
    build_timeline,
    render_dot,
    render_mermaid,
    render_text,
)
from loomwork.failure import FailureInfo
from loomwork.history.events import (
    ChildCompleted,
    ChildScheduled,
    Event,
    SignalReceived,
    StepCompleted,
    StepFailed,
    StepScheduled,
    TickCompleted,
    TimerFired,
    TimerStarted,
    WorkflowCompleted,
    WorkflowStarted,
)

_T0 = datetime(2024, 1, 1, tzinfo=UTC)
_T1 = _T0 + timedelta(seconds=1)
_T2 = _T0 + timedelta(seconds=2)
_T3 = _T0 + timedelta(seconds=3)
_NULL = Payload(b"", {"encoding": ENCODING_NULL})


def _one_of_each_lane() -> list[Event]:
    return [
        WorkflowStarted(
            event_id=1, timestamp=_T0, workflow_name="demo.wf", workflow_id="wf-1", run_id="run-1"
        ),
        TickCompleted(event_id=2, timestamp=_T0, consumed_through=1, time=_T0),
        StepScheduled(event_id=3, timestamp=_T0, seq=1, step_name="charge"),
        StepCompleted(event_id=4, timestamp=_T1, seq=1, result=_NULL),
        TickCompleted(event_id=5, timestamp=_T1, consumed_through=4, time=_T1),
        TimerStarted(event_id=6, timestamp=_T1, seq=1, fire_at=_T2),
        ChildScheduled(
            event_id=7,
            timestamp=_T1,
            seq=1,
            workflow_name="child_wf",
            workflow_id="child-1",
            run_id="child-run-1",
        ),
        TimerFired(event_id=8, timestamp=_T2, seq=1),
        ChildCompleted(event_id=9, timestamp=_T2, seq=1, result=_NULL),
        TickCompleted(event_id=10, timestamp=_T2, consumed_through=9, time=_T2),
        SignalReceived(event_id=11, timestamp=_T3, channel="approvals", payload=_NULL),
        TickCompleted(event_id=12, timestamp=_T3, consumed_through=11, time=_T3),
        WorkflowCompleted(event_id=13, timestamp=_T3, result=_NULL),
    ]


def test_build_timeline_has_one_entry_per_lane_in_start_order():
    entries = build_timeline(_one_of_each_lane())

    assert entries == [
        TimelineEntry("step", "charge", _T0, _T1, "completed", 1),
        TimelineEntry("child", "child_wf (child-1)", _T1, _T2, "completed", 1),
        TimelineEntry("timer", "timer 1", _T1, _T2, "fired", 1),
        TimelineEntry("wait", "channel 'approvals'", _T2, _T3, "resolved", None),
    ]


def test_render_text_golden_output():
    text = render_text(_one_of_each_lane(), title="wf-1 (demo.wf)")

    assert text == (
        "wf-1 (demo.wf)\n"
        "2024-01-01T00:00:00+00:00  step   charge                    completed   "
        "-> 2024-01-01T00:00:01+00:00\n"
        "2024-01-01T00:00:01+00:00  child  child_wf (child-1)        completed   "
        "-> 2024-01-01T00:00:02+00:00\n"
        "2024-01-01T00:00:01+00:00  timer  timer 1                   fired       "
        "-> 2024-01-01T00:00:02+00:00\n"
        "2024-01-01T00:00:02+00:00  wait   channel 'approvals'       resolved    "
        "-> 2024-01-01T00:00:03+00:00"
    )


def test_render_mermaid_golden_output():
    text = render_mermaid(_one_of_each_lane(), title="wf-1 (demo.wf)")

    assert text == (
        "gantt\n"
        "    title wf-1 (demo.wf)\n"
        "    dateFormat  YYYY-MM-DDTHH:mm:ss\n"
        "    section Steps\n"
        "    charge (completed) : done, step_1, 2024-01-01T00:00:00, 1s\n"
        "    section Timers\n"
        "    timer 1 (fired) : done, timer_1, 2024-01-01T00:00:01, 1s\n"
        "    section Children\n"
        "    child_wf (child-1) (completed) : done, child_1, 2024-01-01T00:00:01, 1s\n"
        "    section Waits\n"
        "    channel 'approvals' (resolved) : done, wait_1, 2024-01-01T00:00:02, 1s"
    )


def test_render_dot_golden_output():
    text = render_dot(_one_of_each_lane(), title="wf-1 (demo.wf)")

    # DOT's "line break inside a label" directive is one backslash character
    # followed by "n" -- built from chr(92) so it can't be mistyped as an escape.
    nl = chr(92) + "n"
    assert text == (
        "digraph history {\n"
        '    rankdir="LR";\n'
        "    node [shape=box, style=filled];\n"
        '    label="wf-1 (demo.wf)"; labelloc=t;\n'
        '    subgraph cluster_step { label="Steps";\n'
        f'        step_0 [label="charge{nl}2024-01-01T00:00:00+00:00 -> '
        f'2024-01-01T00:00:01+00:00{nl}completed", fillcolor=palegreen3];\n'
        "    }\n"
        '    subgraph cluster_timer { label="Timers";\n'
        f'        timer_0 [label="timer 1{nl}2024-01-01T00:00:01+00:00 -> '
        f'2024-01-01T00:00:02+00:00{nl}fired", fillcolor=palegreen3];\n'
        "    }\n"
        '    subgraph cluster_child { label="Children";\n'
        f'        child_0 [label="child_wf (child-1){nl}2024-01-01T00:00:01+00:00 -> '
        f'2024-01-01T00:00:02+00:00{nl}completed", fillcolor=palegreen3];\n'
        "    }\n"
        '    subgraph cluster_wait { label="Waits";\n'
        f"        wait_0 [label=\"channel 'approvals'{nl}2024-01-01T00:00:02+00:00 -> "
        f'2024-01-01T00:00:03+00:00{nl}resolved", fillcolor=palegreen3];\n'
        "    }\n"
        "}"
    )


def test_empty_history_renders_a_placeholder_in_every_format():
    empty: list[Event] = []
    assert build_timeline(empty) == []
    assert render_text(empty) == "(no steps, timers, children or waits recorded)"
    assert "no steps, timers, children or waits recorded" in render_mermaid(empty)
    assert (
        render_dot(empty)
        == 'digraph history {\n    rankdir="LR";\n    node [shape=box, style=filled];\n}'
    )


def test_pending_step_has_no_end_and_is_marked_pending():
    history: list[Event] = [
        WorkflowStarted(event_id=1, timestamp=_T0, workflow_name="wf", workflow_id="w", run_id="r"),
        TickCompleted(event_id=2, timestamp=_T0, consumed_through=1, time=_T0),
        StepScheduled(event_id=3, timestamp=_T0, seq=1, step_name="slow"),
    ]

    entries = build_timeline(history)

    assert entries == [TimelineEntry("step", "slow", _T0, None, "pending", 1)]
    assert "-> ..." in render_text(history)


def test_failed_step_is_colored_as_a_failure_in_dot_and_mermaid():
    history: list[Event] = [
        WorkflowStarted(event_id=1, timestamp=_T0, workflow_name="wf", workflow_id="w", run_id="r"),
        TickCompleted(event_id=2, timestamp=_T0, consumed_through=1, time=_T0),
        StepScheduled(event_id=3, timestamp=_T0, seq=1, step_name="risky"),
        StepFailed(
            event_id=4,
            timestamp=_T1,
            seq=1,
            failure=FailureInfo(message="boom", type="ApplicationError"),
        ),
    ]

    assert "fillcolor=firebrick2" in render_dot(history)
    assert ": crit, step_1," in render_mermaid(history)
