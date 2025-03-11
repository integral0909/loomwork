"""Applying a payload codec to events, commands and store records.

The engine applies codecs *at the edges*: workflow code, the deterministic instance and
the replayer always see plain payloads, while everything handed to a store is encoded.
Rather than teaching every event and command type about codecs, this module walks
dataclass instances and transforms any field holding a :class:`Payload`, a list of
payloads, or ``None``. New event types therefore get codec support automatically.

All payloads of a batch of objects go to the codec in a single call, so a codec backed by
a remote key service pays one round trip per tick rather than one per payload.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

from loomwork.codec.base import PayloadCodec
from loomwork.codec.payload import Payload

T = TypeVar("T")
Transform = Callable[[Sequence[Payload]], Awaitable[list[Payload]]]

# (index of object, field name, position in list or None for a single payload)
_Slot = tuple[int, str, int | None]


def _payload_slots(objects: Sequence[Any]) -> tuple[list[_Slot], list[Payload]]:
    slots: list[_Slot] = []
    payloads: list[Payload] = []
    for index, obj in enumerate(objects):
        for f in _dataclass_fields(obj):
            value = getattr(obj, f.name)
            if isinstance(value, Payload):
                slots.append((index, f.name, None))
                payloads.append(value)
            elif isinstance(value, list) and value and all(isinstance(v, Payload) for v in value):
                for position, item in enumerate(value):
                    slots.append((index, f.name, position))
                    payloads.append(item)
    return slots, payloads


def _dataclass_fields(obj: Any) -> tuple[dataclasses.Field[Any], ...]:
    if isinstance(obj, type) or not dataclasses.is_dataclass(obj):
        return ()
    return dataclasses.fields(obj)


async def transform_payloads(objects: Sequence[T], transform: Transform) -> list[T]:
    """Return copies of ``objects`` with every payload field passed through ``transform``.

    Objects without payload fields are returned unchanged (not copied).
    """
    slots, payloads = _payload_slots(objects)
    if not payloads:
        return list(objects)
    transformed = await transform(payloads)
    if len(transformed) != len(payloads):
        raise ValueError(f"codec returned {len(transformed)} payloads for {len(payloads)} inputs")
    changes: dict[int, dict[str, Any]] = {}
    for (index, name, position), payload in zip(slots, transformed, strict=True):
        fields = changes.setdefault(index, {})
        if position is None:
            fields[name] = payload
        else:
            current = fields.get(name)
            if current is None:
                current = list(getattr(objects[index], name))
                fields[name] = current
            current[position] = payload
    out = list(objects)
    for index, fields in changes.items():
        out[index] = dataclasses.replace(objects[index], **fields)  # type: ignore[type-var]
    return out


class CodecBoundary:
    """Encodes objects on their way to a store and decodes them on the way back.

    With no codec configured every method returns its input, so the boundary costs
    nothing when unused.
    """

    def __init__(self, codec: PayloadCodec | None) -> None:
        self.codec = codec

    @property
    def active(self) -> bool:
        return self.codec is not None

    async def encode(self, objects: Sequence[T]) -> list[T]:
        if self.codec is None:
            return list(objects)
        return await transform_payloads(objects, self.codec.encode)

    async def decode(self, objects: Sequence[T]) -> list[T]:
        if self.codec is None:
            return list(objects)
        return await transform_payloads(objects, self.codec.decode)

    async def encode_one(self, obj: T) -> T:
        return (await self.encode([obj]))[0]

    async def decode_one(self, obj: T) -> T:
        return (await self.decode([obj]))[0]

    async def encode_payloads(self, payloads: Sequence[Payload]) -> list[Payload]:
        if self.codec is None:
            return list(payloads)
        return await self.codec.encode(payloads)

    async def decode_payloads(self, payloads: Sequence[Payload]) -> list[Payload]:
        if self.codec is None:
            return list(payloads)
        return await self.codec.decode(payloads)
