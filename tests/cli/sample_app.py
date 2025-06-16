"""Workflow code the CLI tests load with ``--app tests.cli.sample_app``."""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw

approvals = lw.Channel("approvals", str)


@lw.step(name="cli.shout")
async def shout(text: str) -> str:
    return text.upper()


@lw.workflow(name="cli.greet")
async def greet(name: str, punctuation: str = "!") -> str:
    loud = await shout(name)
    return f"HELLO {loud}{punctuation}"


@lw.workflow(name="cli.fail")
async def fail(reason: str) -> None:
    raise lw.ApplicationError(reason, type="Rejected")


@lw.workflow(name="cli.approval")
async def approval(item: str) -> str:
    title = item
    decisions: list[str] = []

    @lw.query("state")
    def state() -> dict[str, object]:
        return {"title": title, "decisions": decisions}

    @lw.update("rename")
    def rename(new_title: str) -> str:
        nonlocal title
        previous, title = title, new_title
        return previous

    decision = await approvals.receive()
    decisions.append(decision)
    return f"{title}: {decision}"


@lw.workflow(name="cli.sleeper")
async def sleeper() -> str:
    await lw.sleep(timedelta(hours=1))
    return "woke"


WORKFLOWS = [greet, fail, approval, sleeper]
STEPS = [shout]


def build() -> list[object]:
    """Used to test ``--app module:callable``."""
    return [*WORKFLOWS, *STEPS]
