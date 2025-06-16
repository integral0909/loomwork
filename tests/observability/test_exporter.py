"""The ``/metrics`` HTTP endpoint."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from loomwork.observability import MetricsRegistry, MetricsServer
from loomwork.observability import exporter as exporter_module


async def request(port: int, raw: bytes) -> tuple[int, dict[str, str], str]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    data = await reader.read()
    writer.close()
    await writer.wait_closed()
    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split(" ")[1])
    headers = dict(line.split(": ", 1) for line in lines[1:])
    return status, headers, body.decode("utf-8")


def get(path: str, method: str = "GET") -> bytes:
    return f"{method} {path} HTTP/1.1\r\nHost: test\r\n\r\n".encode()


@pytest.fixture
async def server() -> AsyncIterator[MetricsServer]:
    registry = MetricsRegistry()
    registry.counter("demo_total", "A demo counter.", ["kind"]).inc(3, kind="x")
    async with MetricsServer(registry, port=0) as running:
        yield running


async def test_serves_prometheus_text(server: MetricsServer) -> None:
    status, headers, body = await request(server.port, get("/metrics"))
    assert status == 200
    assert headers["Content-Type"] == MetricsRegistry.CONTENT_TYPE
    assert int(headers["Content-Length"]) == len(body.encode())
    assert headers["Connection"] == "close"
    assert 'demo_total{kind="x"} 3' in body


async def test_query_string_is_ignored(server: MetricsServer) -> None:
    status, _, body = await request(server.port, get("/metrics?name[]=demo_total"))
    assert status == 200
    assert "demo_total" in body


async def test_health(server: MetricsServer) -> None:
    assert (await request(server.port, get("/healthz")))[0] == 200
    server.ready = lambda: False
    status, _, body = await request(server.port, get("/healthz"))
    assert status == 503
    assert body == "not ready\n"


@pytest.mark.parametrize(
    ("raw", "status"),
    [
        (get("/"), 404),
        (get("/metrics/extra"), 404),
        (get("/metrics", "POST"), 405),
        (b"garbage\r\n\r\n", 400),
        (b"GET /metrics\r\n\r\n", 400),
    ],
)
async def test_errors(server: MetricsServer, raw: bytes, status: int) -> None:
    assert (await request(server.port, raw))[0] == status


async def test_head_request(server: MetricsServer) -> None:
    status, _, _ = await request(server.port, get("/metrics", "HEAD"))
    assert status == 200


async def test_oversized_request_head(server: MetricsServer) -> None:
    raw = b"GET /metrics HTTP/1.1\r\nX-Filler: " + b"a" * 70_000 + b"\r\n\r\n"
    status, _, _ = await request(server.port, raw)
    assert status == 431


async def test_client_that_never_finishes_is_dropped(
    server: MetricsServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(exporter_module, "REQUEST_TIMEOUT", 0.05)
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    writer.write(b"GET /metrics HTTP/1.1\r\n")
    await writer.drain()
    # The server closes the connection without answering once the timeout passes.
    assert await asyncio.wait_for(reader.read(), timeout=2) == b""
    writer.close()
    await writer.wait_closed()
    # And it keeps serving others.
    assert (await request(server.port, get("/metrics")))[0] == 200


async def test_start_and_stop_are_idempotent() -> None:
    metrics = MetricsServer(MetricsRegistry(), port=0)
    await metrics.start()
    port = metrics.port
    await metrics.start()
    assert metrics.port == port
    await metrics.stop()
    await metrics.stop()
    with pytest.raises(OSError):
        await request(port, get("/metrics"))
