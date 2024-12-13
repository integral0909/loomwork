"""Conversion between Python values and :class:`Payload` objects.

The default :class:`DataConverter` encodes values as JSON using pydantic's serializer,
so pydantic models, dataclasses, ``TypedDict``, enums, ``datetime``, ``UUID``, ``Decimal``
and friends all round-trip. Decoding is driven by type hints: when the receiving function
is annotated, the JSON is validated into that type; otherwise plain JSON values come back.
"""

from __future__ import annotations

import inspect
import threading
from collections.abc import Callable, Sequence
from typing import Any, get_type_hints

import pydantic_core
from pydantic import TypeAdapter, ValidationError

from loomwork.codec.payload import (
    ENCODING_BYTES,
    ENCODING_JSON,
    ENCODING_KEY,
    ENCODING_NULL,
    Payload,
)
from loomwork.errors import PayloadError

_NO_HINT: Any = inspect.Parameter.empty


class PayloadConverter:
    """Converts single values. Subclass to support custom encodings."""

    def to_payload(self, value: Any) -> Payload:
        if value is None:
            return Payload(b"", {ENCODING_KEY: ENCODING_NULL})
        if isinstance(value, bytes | bytearray | memoryview):
            return Payload(bytes(value), {ENCODING_KEY: ENCODING_BYTES})
        try:
            data = pydantic_core.to_json(value, by_alias=True, serialize_unknown=False)
        except pydantic_core.PydanticSerializationError as exc:
            raise PayloadError(
                f"cannot encode value of type {type(value).__name__}: {exc}"
            ) from exc
        return Payload(data, {ENCODING_KEY: ENCODING_JSON})

    def from_payload(self, payload: Payload, hint: Any = _NO_HINT) -> Any:
        encoding = payload.encoding
        if encoding == ENCODING_NULL:
            return None
        if encoding == ENCODING_BYTES:
            if hint in (_NO_HINT, Any, bytes, object):
                return payload.data
            return self._adapter(hint).validate_python(payload.data)
        if encoding == ENCODING_JSON:
            if hint in (_NO_HINT, Any, object):
                try:
                    return pydantic_core.from_json(payload.data)
                except ValueError as exc:
                    raise PayloadError(f"payload is not valid JSON: {exc}") from exc
            try:
                return self._adapter(hint).validate_json(payload.data)
            except ValidationError as exc:
                raise PayloadError(f"payload does not match {_hint_name(hint)}: {exc}") from exc
        raise PayloadError(f"unsupported payload encoding {encoding!r}")

    # TypeAdapter construction is expensive; cache per hint. Hints may be unhashable
    # (e.g. Annotated with dict metadata), in which case we build a fresh adapter.
    _adapters: dict[Any, TypeAdapter[Any]]
    _adapters_lock = threading.Lock()

    def __init__(self) -> None:
        self._adapters = {}

    def _adapter(self, hint: Any) -> TypeAdapter[Any]:
        try:
            adapter = self._adapters.get(hint)
        except TypeError:
            return TypeAdapter(hint)
        if adapter is None:
            adapter = TypeAdapter(hint)
            with self._adapters_lock:
                self._adapters[hint] = adapter
        return adapter


class DataConverter:
    """Converts argument lists and results using a :class:`PayloadConverter`.

    Payload *codecs* (compression, encryption) wrap this layer and are added in a later
    milestone; the converter itself only deals with value <-> bytes.
    """

    def __init__(self, payload_converter: PayloadConverter | None = None) -> None:
        self.payload_converter = payload_converter or PayloadConverter()

    def encode(self, value: Any) -> Payload:
        return self.payload_converter.to_payload(value)

    def decode(self, payload: Payload, hint: Any = _NO_HINT) -> Any:
        return self.payload_converter.from_payload(payload, hint)

    def encode_args(self, args: Sequence[Any]) -> list[Payload]:
        return [self.encode(arg) for arg in args]

    def decode_args(self, fn: Callable[..., Any], payloads: Sequence[Payload]) -> list[Any]:
        """Decode positional arguments using ``fn``'s parameter annotations."""
        hints = parameter_hints(fn)
        values = []
        for index, payload in enumerate(payloads):
            hint = hints[index] if index < len(hints) else _NO_HINT
            values.append(self.decode(payload, hint))
        return values

    def decode_result(self, fn: Callable[..., Any], payload: Payload) -> Any:
        return self.decode(payload, return_hint(fn))


def _resolved_hints(fn: Callable[..., Any]) -> dict[str, Any]:
    target = inspect.unwrap(fn)
    try:
        return get_type_hints(target, include_extras=True)
    except (NameError, TypeError):
        # Unresolvable forward references: fall back to untyped decoding.
        return {}


def parameter_hints(fn: Callable[..., Any]) -> list[Any]:
    """Annotations of ``fn``'s positional parameters, in order."""
    hints = _resolved_hints(fn)
    out = []
    for param in inspect.signature(fn).parameters.values():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD, param.KEYWORD_ONLY):
            break
        out.append(hints.get(param.name, _NO_HINT))
    return out


def return_hint(fn: Callable[..., Any]) -> Any:
    hint = _resolved_hints(fn).get("return", _NO_HINT)
    return _NO_HINT if hint is None else hint


def _hint_name(hint: Any) -> str:
    return getattr(hint, "__name__", None) or repr(hint)


default_converter = DataConverter()
