"""Versioning-policy resolution in :class:`~loomwork.engine.config.EngineConfig`."""

from __future__ import annotations

from loomwork.engine.config import EngineConfig
from loomwork.options import VersioningPolicy


class TestResolveVersioningPolicy:
    def test_defaults_to_auto_upgrade(self) -> None:
        config = EngineConfig()
        assert config.resolve_versioning_policy("orders.process") is VersioningPolicy.AUTO_UPGRADE

    def test_default_versioning_policy_applies_to_every_workflow(self) -> None:
        config = EngineConfig(default_versioning_policy=VersioningPolicy.PINNED)
        assert config.resolve_versioning_policy("orders.process") is VersioningPolicy.PINNED
        assert config.resolve_versioning_policy("anything") is VersioningPolicy.PINNED

    def test_per_workflow_override_wins_over_default(self) -> None:
        config = EngineConfig(
            default_versioning_policy=VersioningPolicy.AUTO_UPGRADE,
            workflow_versioning_policy={"orders.process": VersioningPolicy.PINNED},
        )
        assert config.resolve_versioning_policy("orders.process") is VersioningPolicy.PINNED
        assert config.resolve_versioning_policy("other") is VersioningPolicy.AUTO_UPGRADE

    def test_explicit_request_wins_over_everything(self) -> None:
        config = EngineConfig(
            default_versioning_policy=VersioningPolicy.PINNED,
            workflow_versioning_policy={"orders.process": VersioningPolicy.PINNED},
        )
        resolved = config.resolve_versioning_policy("orders.process", VersioningPolicy.AUTO_UPGRADE)
        assert resolved is VersioningPolicy.AUTO_UPGRADE

    def test_pinned_queues_defaults_to_empty(self) -> None:
        assert EngineConfig().pinned_queues == frozenset()
