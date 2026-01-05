"""A seeded asyncio event loop for deterministic simulation.

``asyncio``'s real event loop runs every callback that became ready in a given
iteration in FIFO order (``BaseEventLoop._run_once`` in ``asyncio/base_events.py``
pops them from a ``collections.deque`` with ``popleft``). That is a perfectly good
scheduling policy, but it means a normal test only ever exercises *one* interleaving
of concurrently-ready coroutines: whichever order they happened to be scheduled in.
Two worker dispatch loops woken by the same store notification, the branches of an
``asyncio.gather``, a step's completion racing a lease expiry -- all of these are
"ready at the same time" from the loop's point of view, and FIFO order hides most of
the interleavings that would find a bug.

:class:`SimEventLoop` changes exactly one thing: the order in which *already-ready*
callbacks run within one iteration, drawn from a seeded :class:`random.Random`. Nothing
else about the loop is touched -- timers, I/O polling and callback semantics are
untouched asyncio. This is enough to explore many different interleavings of the same
program under different seeds while keeping each seed's interleaving exactly
reproducible.

``_run_once`` (see the CPython source) only ever calls ``.append()``, ``.popleft()``,
``len()`` and truthiness on ``self._ready``. Replacing that deque with one whose
``popleft`` returns a random element -- instead of reimplementing any part of the loop
-- is therefore sufficient and safe.

**What this makes reproducible.** Given the same seed, the same sequence of
``call_soon``-scheduled callbacks (task steps, future callbacks, ``asyncio.gather``
continuations, ...) that are ready together resume in the same relative order every
time, on the same Python version and platform.

**What this does not make reproducible.** Callbacks scheduled through ``call_later``/
``call_at`` (real timeouts, ``asyncio.sleep`` with a positive delay, socket readiness
from the selector) are still ordered by wall-clock time and the OS's I/O readiness,
which are not reproducible run to run. The simulation harness works around this for its
primary (in-memory) backend by driving a :class:`~loomwork.clock.ManualClock` and never
waiting on a real deadline for workflow-visible time; see ``docs/simulation-testing.md``
for the precise boundary, including what is *not* covered when the harness runs against
a real I/O-bound backend such as SQLite.
"""

from __future__ import annotations

import asyncio
import random
from collections import deque
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

T = TypeVar("T")

__all__ = ["SimEventLoop", "run_simulated"]


class _ShuffledReadyQueue(deque[Any]):
    """Same external behaviour as the deque ``BaseEventLoop`` uses for ``_ready``,
    except ``popleft`` hands back a seeded-random element instead of the oldest one."""

    def __init__(self, rng: random.Random) -> None:
        super().__init__()
        self._rng = rng

    def popleft(self) -> Any:
        if len(self) > 1:
            self.rotate(-self._rng.randrange(len(self)))
        return super().popleft()


class SimEventLoop(asyncio.SelectorEventLoop):
    """A :class:`asyncio.SelectorEventLoop` whose ready-callback order is seeded.

    Use through :func:`run_simulated`, which also installs and tears the loop down
    correctly. Constructing one directly is only useful for tests of the loop itself.
    """

    def __init__(self, seed: int) -> None:
        super().__init__()
        # BaseEventLoop.__init__ already assigned a plain deque; replace it with a
        # shuffled one. Refuse to run if that private detail ever changes, rather than
        # silently simulating nothing.
        if type(getattr(self, "_ready", None)) is not deque:
            raise RuntimeError(
                "this Python's event loop no longer keeps ready callbacks in a deque "
                "named _ready; SimEventLoop needs updating"
            )
        self._ready = _ShuffledReadyQueue(random.Random(seed))


def run_simulated(seed: int, factory: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """Run ``factory()`` to completion on a fresh :class:`SimEventLoop` seeded with ``seed``.

    Creates and disposes of its own loop, so it composes with any test runner (including
    one that manages its own loop per test through pytest-asyncio, since this function
    never touches the loop active before or after it runs).
    """
    # asyncio.Runner with a loop factory never installs the loop as the thread's current
    # loop (so the caller's is not replaced and orphaned), and on exit cancels leftover
    # tasks and shuts down async generators and the default executor before closing.
    with asyncio.Runner(loop_factory=lambda: SimEventLoop(seed)) as runner:
        return runner.run(factory())
