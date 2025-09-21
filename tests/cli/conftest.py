"""Fixtures for CLI tests.

The CLI runs each command with ``asyncio.run``, so these tests are plain synchronous
functions. Workflows started through the CLI are driven to completion by
:func:`drive`, which runs a real worker on its own event loop until a condition holds.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import loomwork as lw
from loomwork.cli import main
from loomwork.store.sqlite import SQLiteStore
from tests.cli import sample_app
from tests.helpers import FAST_CONFIG

APP = "tests.cli.sample_app"


@dataclass
class Result:
    code: int
    out: str
    err: str

    @property
    def lines(self) -> list[str]:
        return self.out.splitlines()


class Cli:
    """Runs ``loomwork`` in-process against one SQLite database."""

    def __init__(self, store_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
        self.store_url = store_url
        monkeypatch.setenv("LOOMWORK_STORE_URL", store_url)
        for name in ("LOOMWORK_SERVER_URL", "LOOMWORK_TOKEN", "LOOMWORK_TOKEN_FILE"):
            monkeypatch.delenv(name, raising=False)

    def __call__(self, *argv: str, stdin: str = "") -> Result:
        out, err = io.StringIO(), io.StringIO()
        code = main(list(argv), stdout=out, stderr=err, stdin=io.StringIO(stdin))
        return Result(code, out.getvalue(), err.getvalue())

    def ok(self, *argv: str, stdin: str = "") -> Result:
        result = self(*argv, stdin=stdin)
        assert result.code == 0, f"exit {result.code}\nstdout: {result.out}\nstderr: {result.err}"
        return result


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "cli.db"


@pytest.fixture
def cli(db_path: Path, monkeypatch: pytest.MonkeyPatch) -> Cli:
    return Cli(f"sqlite:///{db_path}", monkeypatch)


def drive(
    db_path: Path,
    until: Callable[[lw.Loomwork], Awaitable[Any]],
    *,
    timeout: float = 15.0,
) -> Any:
    """Run a worker for the sample app until ``until(app)`` returns."""

    async def run() -> Any:
        store = SQLiteStore(db_path)
        await store.open()
        try:
            app = lw.Loomwork(
                store=store,
                workflows=sample_app.WORKFLOWS,  # type: ignore[arg-type]
                steps=sample_app.STEPS,
                config=FAST_CONFIG,
            )
            async with app:
                return await asyncio.wait_for(until(app), timeout)
        finally:
            await store.close()

    with asyncio.Runner(loop_factory=asyncio.new_event_loop) as runner:
        return runner.run(run())


def finish(db_path: Path, workflow_id: str) -> Any:
    """Run a worker until ``workflow_id`` closes; returns nothing, errors are ignored."""

    async def closed(app: lw.Loomwork) -> None:
        with contextlib.suppress(lw.LoomworkError):
            await app.get_handle(workflow_id).result()

    return drive(db_path, closed)


def run_with_worker(db_path: Path, cli: Cli, *argv: str) -> Result:
    """Run a CLI command on a thread while a worker executes workflows, for commands that
    wait for the workflow to do something (``--wait``, updates)."""
    outcome: list[Result] = []
    thread = threading.Thread(target=lambda: outcome.append(cli(*argv)))

    async def until_done(_app: lw.Loomwork) -> None:
        thread.start()
        await asyncio.to_thread(thread.join)

    drive(db_path, until_done)
    return outcome[0]
