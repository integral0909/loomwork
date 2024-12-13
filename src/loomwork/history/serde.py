"""JSON-compatible (de)serialization of history events.

The wire format of an event is a flat object::

    {"id": 7, "ts": "2024-05-01T12:00:00+00:00", "kind": "step_completed",
     "seq": 2, "result": {"data": "...", "metadata": {...}}, "attempts": 1}

Field codecs are derived once per event class from its type annotations, so adding a
field to an event only requires a default value to stay backward compatible: unknown
fields in stored data are ignored, missing fields fall back to the dataclass default.
"""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

from loomwork.codec.payload import Payload
from loomwork.failure import FailureInfo
from loomwork.history.events import EPOCH, EVENT_TYPES, HISTORY_FORMAT_VERSION, Event
from loomwork.options import StepOptions

Encoder = Callable[[Any], Any]
Decoder = Callable[[Any], Any]

_BASE_FIELDS = {"event_id", "timestamp"}


class HistoryFormatError(ValueError):
    """Stored event data could not be decoded."""


def _identity(value: Any) -> Any:
    return value


def _encode_datetime(value: datetime) -> str:
    return value.isoformat()


def _decode_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise HistoryFormatError(f"naive timestamp {value!r} in history")
    return parsed


_SIMPLE: dict[Any, tuple[Encoder, Decoder]] = {
    Payload: (lambda p: p.to_dict(), Payload.from_dict),
    FailureInfo: (lambda f: f.to_dict(), FailureInfo.from_dict),
    StepOptions: (lambda o: o.to_dict(), StepOptions.from_dict),
    datetime: (_encode_datetime, _decode_datetime),
}


def _codec_for(hint: Any) -> tuple[Encoder, Decoder]:
    if hint in _SIMPLE:
        return _SIMPLE[hint]
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)
    if origin in (typing.Union, types.UnionType):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            enc, dec = _codec_for(non_none[0])
            return (
                lambda v: None if v is None else enc(v),
                lambda v: None if v is None else dec(v),
            )
        return _identity, _identity
    if origin is list and args:
        item_enc, item_dec = _codec_for(args[0])
        if item_enc is _identity:
            return list, list
        return (
            lambda v: [item_enc(i) for i in v],
            lambda v: [item_dec(i) for i in v],
        )
    return _identity, _identity


_FIELD_CODECS: dict[type[Event], tuple[tuple[str, Encoder, Decoder], ...]] = {}


def _field_codecs(cls: type[Event]) -> tuple[tuple[str, Encoder, Decoder], ...]:
    cached = _FIELD_CODECS.get(cls)
    if cached is not None:
        return cached
    hints = typing.get_type_hints(cls)
    codecs = []
    for f in dataclasses.fields(cls):
        if f.name in _BASE_FIELDS:
            continue
        enc, dec = _codec_for(hints[f.name])
        codecs.append((f.name, enc, dec))
    result = tuple(codecs)
    _FIELD_CODECS[cls] = result
    return result


def event_to_dict(event: Event) -> dict[str, Any]:
    data: dict[str, Any] = {
        "id": event.event_id,
        "ts": event.timestamp.isoformat(),
        "kind": event.kind,
    }
    for name, enc, _ in _field_codecs(type(event)):
        data[name] = enc(getattr(event, name))
    return data


def event_from_dict(data: dict[str, Any]) -> Event:
    kind = data.get("kind")
    cls = EVENT_TYPES.get(str(kind))
    if cls is None:
        raise HistoryFormatError(f"unknown event kind {kind!r}")
    kwargs: dict[str, Any] = {
        "event_id": int(data.get("id", 0)),
        "timestamp": _decode_datetime(data["ts"]) if "ts" in data else EPOCH,
    }
    try:
        for name, _, dec in _field_codecs(cls):
            if name in data:
                kwargs[name] = dec(data[name])
        return cls(**kwargs)
    except (KeyError, TypeError, ValueError) as exc:
        raise HistoryFormatError(f"malformed {kind} event: {exc}") from exc


def event_to_json(event: Event) -> str:
    return json.dumps(event_to_dict(event), separators=(",", ":"), sort_keys=True)


def event_from_json(text: str | bytes) -> Event:
    return event_from_dict(json.loads(text))


def history_to_json(events: Iterable[Event], *, indent: int | None = 2) -> str:
    """Export a history as a JSON document (used for replay files and debugging)."""
    doc = {
        "format_version": HISTORY_FORMAT_VERSION,
        "events": [event_to_dict(e) for e in events],
    }
    return json.dumps(doc, indent=indent, sort_keys=True)


def history_from_json(text: str | bytes) -> list[Event]:
    doc = json.loads(text)
    version = doc.get("format_version")
    if version != HISTORY_FORMAT_VERSION:
        raise HistoryFormatError(
            f"unsupported history format version {version!r} (expected {HISTORY_FORMAT_VERSION})"
        )
    return [event_from_dict(item) for item in doc["events"]]
