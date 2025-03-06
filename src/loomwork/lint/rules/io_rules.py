"""LW103-LW108: I/O that belongs in a step, not in workflow code.

Workflow code only ever *decides*; it must not touch a file, a socket, a subprocess or a
database, because none of that is journaled and none of it can be replayed. Each concern
gets its own code (rather than one big "I/O" rule) because the fix and the severity
differ: ``print`` is a warning with an easy fix (use ``loomwork.logger``), while a raw
socket call is an error with no shortcut other than "move it into a step".
"""

from __future__ import annotations

from loomwork.lint.models import Severity
from loomwork.lint.registry import DEFAULT_REGISTRY, Rule
from loomwork.lint.rules.tables import ForbiddenCall, MatchKind

_STEP_HINT = "move this call into a @step function and await it from the workflow"

RULE_FILESYSTEM_IO = DEFAULT_REGISTRY.register(
    Rule(
        code="LW103",
        name="filesystem-io",
        severity=Severity.ERROR,
        message="{name}() touches the filesystem, which is not journaled",
        hint=_STEP_HINT,
    )
)

RULE_PRINT = DEFAULT_REGISTRY.register(
    Rule(
        code="LW104",
        name="print-call",
        severity=Severity.WARNING,
        message="print() output is not part of the workflow's history",
        hint="use loomwork.logger instead; it is suppressed automatically during replay",
    )
)

RULE_STDIN = DEFAULT_REGISTRY.register(
    Rule(
        code="LW105",
        name="blocking-stdin",
        severity=Severity.ERROR,
        message="{name}() blocks on interactive input, which no replay can supply",
        hint=_STEP_HINT,
    )
)

RULE_NETWORK_IO = DEFAULT_REGISTRY.register(
    Rule(
        code="LW106",
        name="network-io",
        severity=Severity.ERROR,
        message="{name}() performs network I/O, whose outcome cannot be replayed",
        hint=_STEP_HINT,
    )
)

RULE_SUBPROCESS_IO = DEFAULT_REGISTRY.register(
    Rule(
        code="LW107",
        name="subprocess-io",
        severity=Severity.ERROR,
        message="{name}() runs an external process, whose outcome cannot be replayed",
        hint=_STEP_HINT,
    )
)

RULE_DATABASE_IO = DEFAULT_REGISTRY.register(
    Rule(
        code="LW108",
        name="database-io",
        severity=Severity.ERROR,
        message="{name}() talks to a database directly from workflow code",
        hint=_STEP_HINT,
    )
)

_PATH_METHODS = (
    "read_text",
    "read_bytes",
    "write_text",
    "write_bytes",
    "open",
    "exists",
    "is_file",
    "is_dir",
    "is_symlink",
    "mkdir",
    "rmdir",
    "unlink",
    "iterdir",
    "glob",
    "rglob",
    "stat",
    "lstat",
    "chmod",
    "touch",
    "rename",
    "replace",
    "symlink_to",
    "hardlink_to",
    "samefile",
    "walk",
)

_OS_PATH_FUNCTIONS = (
    "exists",
    "isfile",
    "isdir",
    "islink",
    "getsize",
    "getmtime",
    "getctime",
    "samefile",
)

_OS_FS_FUNCTIONS = (
    "listdir",
    "scandir",
    "walk",
    "remove",
    "unlink",
    "rmdir",
    "mkdir",
    "makedirs",
    "removedirs",
    "rename",
    "replace",
    "stat",
    "lstat",
    "chmod",
    "chown",
    "open",
    "symlink",
    "link",
    # ``os.system`` / ``os.popen`` are subprocess execution, not filesystem access; they
    # are registered under LW107 below instead.
)

FORBIDDEN_CALLS = (
    ForbiddenCall("builtins.open", RULE_FILESYSTEM_IO.code),
    *(ForbiddenCall(f"pathlib.Path.{method}", RULE_FILESYSTEM_IO.code) for method in _PATH_METHODS),
    *(ForbiddenCall(f"os.path.{fn}", RULE_FILESYSTEM_IO.code) for fn in _OS_PATH_FUNCTIONS),
    *(ForbiddenCall(f"os.{fn}", RULE_FILESYSTEM_IO.code) for fn in _OS_FS_FUNCTIONS),
    ForbiddenCall("shutil", RULE_FILESYSTEM_IO.code, MatchKind.PREFIX),
    ForbiddenCall("builtins.print", RULE_PRINT.code),
    ForbiddenCall("builtins.input", RULE_STDIN.code),
    ForbiddenCall("sys.stdin.read", RULE_STDIN.code),
    ForbiddenCall("sys.stdin.readline", RULE_STDIN.code),
    ForbiddenCall("socket", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("requests", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("httpx", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("aiohttp", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("urllib.request", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("http.client", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("ftplib", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("smtplib", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("websocket", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("websockets", RULE_NETWORK_IO.code, MatchKind.PREFIX),
    ForbiddenCall("subprocess", RULE_SUBPROCESS_IO.code, MatchKind.PREFIX),
    ForbiddenCall("os.system", RULE_SUBPROCESS_IO.code),
    ForbiddenCall("os.popen", RULE_SUBPROCESS_IO.code),
    ForbiddenCall("sqlite3", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("psycopg", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("psycopg2", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("asyncpg", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("pymongo", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("motor", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("redis", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("pymysql", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("mysql.connector", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("pyodbc", RULE_DATABASE_IO.code, MatchKind.PREFIX),
    ForbiddenCall("sqlalchemy.create_engine", RULE_DATABASE_IO.code, MatchKind.PREFIX),
)
