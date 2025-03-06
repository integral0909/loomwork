"""Unit tests for dotted-name resolution, independent of any rule."""

from __future__ import annotations

import ast
import textwrap

from loomwork.lint.resolving import (
    build_import_map,
    classify_decorators,
    resolve_call_target,
    resolve_dotted,
)


def _parse(source: str) -> ast.Module:
    return ast.parse(textwrap.dedent(source))


class TestBuildImportMap:
    def test_plain_import_binds_the_top_level_package(self) -> None:
        tree = _parse("import os.path\n")
        assert build_import_map(tree) == {"os": "os"}

    def test_import_as_binds_the_full_dotted_path(self) -> None:
        tree = _parse("import loomwork.sdk as sdk\n")
        assert build_import_map(tree) == {"sdk": "loomwork.sdk"}

    def test_from_import_binds_the_member(self) -> None:
        tree = _parse("from datetime import datetime\n")
        assert build_import_map(tree) == {"datetime": "datetime.datetime"}

    def test_from_import_as_binds_the_alias(self) -> None:
        tree = _parse("from loomwork import workflow as wf\n")
        assert build_import_map(tree) == {"wf": "loomwork.workflow"}

    def test_star_import_is_ignored(self) -> None:
        tree = _parse("from os import *\n")
        assert build_import_map(tree) == {}

    def test_relative_import_is_ignored(self) -> None:
        tree = _parse("from . import sibling\n")
        assert build_import_map(tree) == {}

    def test_import_inside_a_function_is_not_captured(self) -> None:
        tree = _parse(
            """
            def f():
                import time
            """
        )
        assert build_import_map(tree) == {}


class TestResolveDotted:
    def test_resolves_a_bare_imported_name(self) -> None:
        names = {"time": "time"}
        node = ast.parse("time", mode="eval").body
        assert resolve_dotted(node, names) == "time"

    def test_resolves_an_attribute_chain(self) -> None:
        names = {"dt": "datetime.datetime"}
        node = ast.parse("dt.now", mode="eval").body
        assert resolve_dotted(node, names) == "datetime.datetime.now"

    def test_unresolved_base_yields_none(self) -> None:
        node = ast.parse("mystery.attr", mode="eval").body
        assert resolve_dotted(node, {}) is None

    def test_resolves_through_an_intermediate_call(self) -> None:
        names = {"requests": "requests"}
        node = ast.parse("requests.Session().get", mode="eval").body
        assert resolve_dotted(node, names) == "requests.Session.get"


class TestResolveCallTarget:
    def test_resolves_an_imported_call(self) -> None:
        names = {"time": "time"}
        call = ast.parse("time.time()", mode="eval").body
        assert isinstance(call, ast.Call)
        assert resolve_call_target(call, names) == "time.time"

    def test_falls_back_to_builtins_for_unshadowed_names(self) -> None:
        call = ast.parse("open('f')", mode="eval").body
        assert isinstance(call, ast.Call)
        assert resolve_call_target(call, {}) == "builtins.open"

    def test_does_not_fall_back_for_unknown_bare_names(self) -> None:
        call = ast.parse("mystery()", mode="eval").body
        assert isinstance(call, ast.Call)
        assert resolve_call_target(call, {}) is None


class TestClassifyDecorators:
    def test_classifies_workflow(self) -> None:
        tree = _parse(
            """
            import loomwork as lw

            @lw.workflow
            async def f(): ...
            """
        )
        names = build_import_map(tree)
        fn = tree.body[-1]
        assert isinstance(fn, ast.AsyncFunctionDef)
        assert classify_decorators(fn, names) == "workflow"

    def test_classifies_workflow_called_with_arguments(self) -> None:
        tree = _parse(
            """
            import loomwork as lw

            @lw.workflow(name="x")
            async def f(): ...
            """
        )
        names = build_import_map(tree)
        fn = tree.body[-1]
        assert isinstance(fn, ast.AsyncFunctionDef)
        assert classify_decorators(fn, names) == "workflow"

    def test_classifies_step(self) -> None:
        tree = _parse(
            """
            import loomwork as lw

            @lw.step
            async def f(): ...
            """
        )
        names = build_import_map(tree)
        fn = tree.body[-1]
        assert isinstance(fn, ast.AsyncFunctionDef)
        assert classify_decorators(fn, names) == "step"

    def test_unrelated_decorator_classifies_as_none(self) -> None:
        tree = _parse(
            """
            import functools

            @functools.cache
            def f(): ...
            """
        )
        names = build_import_map(tree)
        fn = tree.body[-1]
        assert isinstance(fn, ast.FunctionDef)
        assert classify_decorators(fn, names) is None
