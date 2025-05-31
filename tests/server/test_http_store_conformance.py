"""Runs the full store conformance suite against :class:`HttpStore` talking to an
in-process server (``httpx.ASGITransport``, no real network) fronting first a
:class:`~loomwork.store.memory.MemoryStore` and then a
:class:`~loomwork.store.sqlite.SQLiteStore`.

**Why every assertion still holds despite the server's clock rule.** The conformance
suite calls store methods with concrete absolute timestamps built from a fixed
``tests.helpers.T0`` (for example ``store.claim_tick(worker, T0 + timedelta(seconds=5),
lease)``) and asserts on equally concrete results (``claimed.lease_expires_at == T0 +
LEASE``). Per ``docs/server.md``, ``HttpStore`` never sends an absolute time: every
datetime argument is converted to an offset in seconds from ``self.clock.now()`` taken at
the moment of the call, and the server reconstructs it as ``server_clock.now() +
offset``.

The ``store`` and ``sqlite_store`` fixtures (in ``conftest.py``) give the ``HttpStore`` and
the server the *same* :class:`~loomwork.clock.ManualClock` object, frozen at ``T0`` and
never advanced by anything in this test module (no system worker runs against these
namespaces, so nothing but an explicit ``clock.advance()``/``clock.set()`` call -- which
this suite never makes -- would move it). Both sides therefore read exactly the same
"now" for every request, so ``offset = desired - T0`` and ``server_now + offset == T0 +
offset == desired``: the round trip is exact, not approximate, which is what lets the
suite's exact-equality assertions pass unchanged.

This is also why the in-process server here disables its per-namespace system worker
(``run_system_workers=False``): the suite calls ``fire_due_timers``/``relay_outbox``
directly to control exactly when a timer fires or a message is relayed, and a background
worker doing the same thing concurrently would race those explicit calls. A production
server enables the system worker (the default) precisely so that no external caller has
to do this; see ``docs/server.md`` for that trade-off.
"""

from __future__ import annotations

import pytest

from loomwork.remote.http_store import HttpStore
from tests.store_conformance import StoreConformance


class TestHttpStoreOverMemory(StoreConformance):
    """``store`` comes from ``tests/server/conftest.py``: HttpStore -> ASGI -> MemoryStore."""


class TestHttpStoreOverSQLite(StoreConformance):
    @pytest.fixture
    def store(self, sqlite_store: HttpStore) -> HttpStore:
        return sqlite_store
