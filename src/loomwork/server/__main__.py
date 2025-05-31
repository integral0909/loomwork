"""``python -m loomwork.server --config path.toml``

A minimal CLI entry point; a full ``loomwork`` command-line tool (with subcommands for
generating tokens, running migrations, and so on) is expected to wrap :func:`serve` and
:func:`~loomwork.server.auth.generate_token` directly rather than shelling out to this.
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
