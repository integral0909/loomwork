"""LW103-LW108: I/O that belongs in a step."""

from __future__ import annotations

from tests.lint.helpers import codes_of, lint


class TestFilesystemIo:
    def test_open_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                open("f.txt")
            """
        )
        assert codes_of(findings) == ["LW103"]

    def test_pathlib_read_and_write_are_flagged(self) -> None:
        findings = lint(
            """
            from pathlib import Path
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                p = Path("/tmp/x")
                p.write_text("hi")
                Path("/tmp/y").read_text()
            """
        )
        assert codes_of(findings) == ["LW103", "LW103"]

    def test_os_filesystem_functions_are_flagged(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                os.listdir(".")
                os.remove("f.txt")
            """
        )
        assert codes_of(findings) == ["LW103", "LW103"]

    def test_shutil_is_flagged(self) -> None:
        findings = lint(
            """
            import shutil
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                shutil.copy("a", "b")
            """
        )
        assert codes_of(findings) == ["LW103"]

    def test_pure_path_construction_is_not_flagged(self) -> None:
        # Building and joining paths is pure computation; only touching disk is I/O.
        findings = lint(
            """
            from pathlib import Path
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                p = Path("a") / "b"
                str(p)
            """
        )
        assert findings == []


class TestPrint:
    def test_print_is_a_warning(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                print("hi")
            """
        )
        assert codes_of(findings) == ["LW104"]
        assert findings[0].severity.value == "warning"


class TestStdin:
    def test_input_is_flagged(self) -> None:
        findings = lint(
            """
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                input("continue? ")
            """
        )
        assert codes_of(findings) == ["LW105"]


class TestNetworkIo:
    def test_requests_get_is_flagged(self) -> None:
        findings = lint(
            """
            import requests
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                requests.get("https://example.com")
            """
        )
        assert codes_of(findings) == ["LW106"]

    def test_requests_session_chained_call_is_flagged(self) -> None:
        # Both the Session() construction and the chained .get() resolve into the
        # "requests" module and are each reported.
        findings = lint(
            """
            import requests
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                requests.Session().get("https://example.com")
            """
        )
        assert codes_of(findings) == ["LW106", "LW106"]

    def test_socket_via_variable_is_flagged(self) -> None:
        findings = lint(
            """
            import socket
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                s = socket.socket()
                s.connect(("example.com", 80))
            """
        )
        assert codes_of(findings) == ["LW106", "LW106"]

    def test_httpx_and_aiohttp_are_flagged(self) -> None:
        findings = lint(
            """
            import httpx
            import aiohttp
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                httpx.get("https://example.com")
                aiohttp.ClientSession()
            """
        )
        assert codes_of(findings) == ["LW106", "LW106"]


class TestSubprocessIo:
    def test_subprocess_run_is_flagged(self) -> None:
        findings = lint(
            """
            import subprocess
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                subprocess.run(["ls"])
            """
        )
        assert codes_of(findings) == ["LW107"]

    def test_os_system_and_popen_are_flagged(self) -> None:
        findings = lint(
            """
            import os
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                os.system("ls")
                os.popen("ls")
            """
        )
        assert codes_of(findings) == ["LW107", "LW107"]


class TestDatabaseIo:
    def test_sqlite3_connect_and_chained_execute_are_flagged(self) -> None:
        findings = lint(
            """
            import sqlite3
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                conn = sqlite3.connect("db.sqlite")
                conn.execute("SELECT 1")
            """
        )
        assert codes_of(findings) == ["LW108", "LW108"]

    def test_sqlalchemy_create_engine_is_flagged(self) -> None:
        findings = lint(
            """
            import sqlalchemy
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                sqlalchemy.create_engine("sqlite://")
            """
        )
        assert codes_of(findings) == ["LW108"]

    def test_redis_and_pymongo_are_flagged(self) -> None:
        findings = lint(
            """
            import redis
            import pymongo
            import loomwork as lw

            @lw.workflow
            async def process() -> None:
                redis.Redis()
                pymongo.MongoClient()
            """
        )
        assert codes_of(findings) == ["LW108", "LW108"]
