"""``python -m loomwork.server --config path.toml``

Equivalent to ``loomwork server --config path.toml``, kept for environments that run
modules rather than console scripts.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys

from loomwork.server.app import serve
from loomwork.server.config import load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m loomwork.server")
    parser.add_argument("--config", required=True, help="path to the server's TOML config file")
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="root log level (default: info)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config(args.config)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(config))
    return 0


if __name__ == "__main__":
    sys.exit(main())
