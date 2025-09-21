"""A tiny workflow used across archive tests."""

from __future__ import annotations

from datetime import timedelta

import loomwork as lw


@lw.step(name="arc.step")
async def shout(text: str) -> str:
    return text.upper()


@lw.workflow(name="arc.wf")
async def greet(text: str) -> str:
    return await shout(text)


@lw.workflow(name="arc.sleeper")
async def sleeper(text: str) -> str:
    await lw.sleep(timedelta(hours=1))
    return await shout(text)


@lw.workflow(name="arc.failer")
async def failer(reason: str) -> None:
    raise lw.ApplicationError(reason, type="Rejected")


WORKFLOWS = [greet, sleeper, failer]
STEPS = [shout]
