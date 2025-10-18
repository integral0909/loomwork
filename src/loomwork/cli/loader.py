"""Import the user's workflow code named on the command line.

Commands that execute or replay workflow code (``worker``, ``compat``, ``workflow
query``) take an *app reference*:

``package.module``
    every ``@workflow`` (function or class) and ``@step`` defined at the top level of the
    module.
``package.module:name``
    ``name`` is a :class:`~loomwork.Loomwork` app (its registry and converter are used),
    a :class:`~loomwork.sdk.definitions.Registry`, a list of definitions, or a function
    taking no arguments that returns one of those.
``path/to/file.py`` or ``path/to/file.py:name``
    the same, for a file that is not importable as a module.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from loomwork.cli.errors import UsageError
from loomwork.codec.converter import DataConverter, default_converter
from loomwork.sdk.definitions import Registry, StepDefinition, WorkflowDefinition
from loomwork.sdk.handlers import DEFINITION_ATTRIBUTE


@dataclass(frozen=True, slots=True)
class AppSpec:
    """Workflow code loaded from an app reference."""

    reference: str
    registry: Registry
    converter: DataConverter

    @property
    def workflow_names(self) -> list[str]:
        return sorted(self.registry.workflow_names)

    @property
    def step_names(self) -> list[str]:
        return sorted(self.registry.step_names)


def _split(reference: str) -> tuple[str, str | None]:
    # Windows drive letters ("C:\\app.py") contain a colon; only split on the last one
    # and only when what follows looks like an attribute name.
    target, sep, attribute = reference.rpartition(":")
    if sep and attribute.isidentifier() and target:
        return target, attribute
    return reference, None


def _import_file(path: Path) -> ModuleType:
    if not path.is_file():
        raise UsageError(f"file not found: {path}")
    name = f"_loomwork_app_{path.stem}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise UsageError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered before execution so dataclasses and pickling can find the module.
    sys.modules[name] = module
    # Like ``python file.py``: the file's directory stays importable, so modules it
    # imports lazily (inside steps, for example) are found later too.
    directory = str(path.resolve().parent)
    if directory not in sys.path:
        sys.path.insert(0, directory)
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


def _import_module(name: str) -> ModuleType:
    # The current directory is importable, as with ``python -m``.
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        if exc.name is not None and (name == exc.name or name.startswith(exc.name + ".")):
            raise UsageError(f"cannot import module {name!r}: {exc}") from None
        raise


def _is_workflow_class(value: Any) -> bool:
    return isinstance(value, type) and DEFINITION_ATTRIBUTE in vars(value)


def _definitions(values: Iterable[Any]) -> Registry:
    registry = Registry()
    for value in values:
        if isinstance(value, WorkflowDefinition) or _is_workflow_class(value):
            registry.add_workflow(value)
        elif isinstance(value, StepDefinition):
            registry.add_step(value)
    return registry


def _from_object(reference: str, value: Any) -> tuple[Registry, DataConverter]:
    from loomwork.app import Loomwork

    if isinstance(value, Loomwork):
        return value.registry, value.converter
    if isinstance(value, Registry):
        return value, default_converter
    if isinstance(value, WorkflowDefinition | StepDefinition) or _is_workflow_class(value):
        return _definitions([value]), default_converter
    if isinstance(value, list | tuple | set | frozenset):
        return _definitions(value), default_converter
    if callable(value):
        return _from_object(reference, value())
    raise UsageError(
        f"{reference} is a {type(value).__name__}; expected a Loomwork app, a Registry, "
        "a list of definitions, or a function returning one"
    )


def load_app(reference: str) -> AppSpec:
    """Import ``reference`` and collect its workflow and step definitions."""
    target, attribute = _split(reference)
    if target.endswith(".py") or "/" in target or "\\" in target:
        module = _import_file(Path(target))
    else:
        module = _import_module(target)

    if attribute is None:
        registry = _definitions(vars(module).values())
        converter = default_converter
    else:
        try:
            value = getattr(module, attribute)
        except AttributeError:
            raise UsageError(f"{target} has no attribute {attribute!r}") from None
        registry, converter = _from_object(reference, value)

    spec = AppSpec(reference, registry, converter)
    if not spec.workflow_names and not spec.step_names:
        raise UsageError(f"{reference} defines no workflows or steps")
    return spec
