"""Every example's ``main.py`` runs to completion on its own, in a clean process."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_NAMES = sorted(
    path.name
    for path in (REPO_ROOT / "examples").iterdir()
    if path.is_dir() and (path / "main.py").is_file()
)


@pytest.mark.parametrize("example_name", EXAMPLE_NAMES)
def test_main_exits_cleanly(example_name: str) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", f"examples.{example_name}.main"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, (
        f"examples.{example_name}.main exited {completed.returncode}\n"
        f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}"
    )


def test_every_example_package_was_discovered() -> None:
    # A canary against a typo silently dropping an example from the parametrization above.
    assert set(EXAMPLE_NAMES) == {
        "order_fulfillment",
        "expense_approval",
        "bulk_import",
        "scheduled_reports",
        "task_queues",
        "encrypted_payments",
        "versioned_workflow",
        "support_ticket",
        "blue_green_deploy",
        "traced_pipeline",
        "media_processing",
        "incident_recovery",
    }
