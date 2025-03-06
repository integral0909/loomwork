"""Entry point for ``python -m loomwork.lint``."""

from __future__ import annotations

import sys

from loomwork.lint.cli import main

if __name__ == "__main__":
    sys.exit(main())
