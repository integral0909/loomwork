"""Wires the ``loomwork.testing`` pytest plugin into this package's own tests.

This is exactly what a project embedding Loomwork is expected to do in its own
``conftest.py`` (see ``docs/testing.md``); doing it here both exercises the plugin and
gives this package's tests the ``loomwork_env`` fixture.
"""

from __future__ import annotations

pytest_plugins = ["loomwork.testing.pytest_plugin"]
