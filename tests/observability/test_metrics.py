"""The metrics registry and Prometheus exposition format."""

from __future__ import annotations

import math
import threading

import pytest

from loomwork.observability import MetricsError, MetricsRegistry


def test_counter_and_labels() -> None:
    registry = MetricsRegistry()
    requests = registry.counter("app_requests_total", "Requests handled.", ["method"])
    requests.inc(method="GET")
    requests.inc(2, method="GET")
    requests.inc(method="POST")
    assert requests.value(method="GET") == 3
    text = registry.render()
    assert "# HELP app_requests_total Requests handled." in text
    assert "# TYPE app_requests_total counter" in text
    assert 'app_requests_total{method="GET"} 3' in text
    assert 'app_requests_total{method="POST"} 1' in text


def test_counter_rejects_decrease_and_wrong_labels() -> None:
    counter = MetricsRegistry().counter("c_total", "c", ["a"])
    with pytest.raises(MetricsError, match="only increase"):
        counter.inc(-1, a="x")
    with pytest.raises(MetricsError, match="expects labels"):
        counter.inc(b="x")


def test_gauge() -> None:
    registry = MetricsRegistry()
    gauge = registry.gauge("queue_depth", "Depth.")
    gauge.set(5)
    gauge.inc()
    gauge.dec(3)
    assert gauge.value() == 3
    assert "queue_depth 3" in registry.render()


def test_histogram_buckets_are_cumulative() -> None:
    registry = MetricsRegistry()
    hist = registry.histogram("latency_seconds", "Latency.", ["op"], buckets=[0.1, 1.0])
    for value in (0.05, 0.5, 0.5, 2.0):
        hist.observe(value, op="read")
    text = registry.render()
    assert 'latency_seconds_bucket{op="read",le="0.1"} 1' in text
    assert 'latency_seconds_bucket{op="read",le="1"} 3' in text
    assert 'latency_seconds_bucket{op="read",le="+Inf"} 4' in text
    assert 'latency_seconds_count{op="read"} 4' in text
    assert hist.count(op="read") == 4
    assert math.isclose(hist.sum(op="read"), 3.05)


def test_label_values_are_escaped() -> None:
    registry = MetricsRegistry()
    counter = registry.counter("odd_total", "Help with \\ and\nnewline.", ["v"])
    counter.inc(v='quote " back \\ slash\nline')
    text = registry.render()
    assert r'odd_total{v="quote \" back \\ slash\nline"} 1' in text
    assert r"# HELP odd_total Help with \\ and\nnewline." in text


def test_registration_rules() -> None:
    registry = MetricsRegistry()
    first = registry.counter("x_total", "x", ["a"])
    assert registry.counter("x_total", "x", ["a"]) is first
    with pytest.raises(MetricsError, match="registered differently"):
        registry.gauge("x_total", "x", ["a"])
    with pytest.raises(MetricsError, match="invalid metric name"):
        registry.counter("bad-name", "x")
    with pytest.raises(MetricsError, match="invalid label"):
        registry.counter("y_total", "y", ["__reserved"])
    with pytest.raises(MetricsError, match="duplicate"):
        registry.counter("z_total", "z", ["a", "a"])
    with pytest.raises(MetricsError, match="reserved"):
        registry.histogram("h", "h", ["le"])
    assert registry.get("x_total") is first
    assert registry.get("nope") is None


def test_render_is_sorted_and_ends_with_newline() -> None:
    registry = MetricsRegistry()
    registry.gauge("b_metric", "b").set(1)
    registry.gauge("a_metric", "a").set(1.5)
    text = registry.render()
    assert text.index("a_metric") < text.index("b_metric")
    assert "a_metric 1.5" in text
    assert text.endswith("\n")


def test_thread_safety() -> None:
    counter = MetricsRegistry().counter("n_total", "n")

    def work() -> None:
        for _ in range(10_000):
            counter.inc()

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert counter.value() == 80_000
