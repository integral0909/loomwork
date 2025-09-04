"""The console's Jinja2 environment.

Autoescaping is always on (every template is ``.html``), and nothing in this module
ever marks a value ``|safe`` -- workflow ids, signal payloads and every other operator-
or workflow-controlled string are rendered through the ordinary autoescaped ``{{ }}``
syntax, which is what keeps a hostile value like ``<script>`` inert on the page.
"""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from loomwork.server.console.formatting import describe_payload, describe_value

TEMPLATES_DIR = Path(__file__).parent / "templates"


def build_environment() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"], default_for_string=True),
        trim_blocks=True,
        lstrip_blocks=True,
        auto_reload=False,
    )
    env.filters["describe_value"] = describe_value
    env.filters["describe_payload"] = describe_payload
    return env
