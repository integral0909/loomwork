"""Codec overhead: throughput of the same one-step workflow through no codec,
compression, encryption, and both, plus offloading on top -- see
``docs/payload-encryption.md`` and ``docs/large-payloads.md``. The payload is
compressible text large enough (4 KiB) for compression's threshold and offload's
default threshold to both matter.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

from benchmarks.appctx import running_app
from benchmarks.harness import BenchContext, Case, Scenario, Trial
from benchmarks.workloads import make_payload, simple_workflow
from loomwork.blobs.memory import MemoryBlobStore
from loomwork.codec import (
    CodecChain,
    CompressionCodec,
    DataConverter,
    EncryptionCodec,
    StaticKeyRing,
)
from loomwork.codec.offload import OffloadCodec

_PAYLOAD_BYTES = 4096
_OFFLOAD_THRESHOLD = 1024
_KEY_ID = "bench"
_KEY_MATERIAL = b"\x00" * 32  # fixed, non-secret: benchmark data never leaves this process


def _converter(codec_name: str) -> DataConverter:
    keyring = StaticKeyRing({_KEY_ID: _KEY_MATERIAL}, current=_KEY_ID)
    match codec_name:
        case "none":
            return DataConverter()
        case "compression":
            return DataConverter(codec=CompressionCodec())
        case "encryption":
            return DataConverter(codec=EncryptionCodec(keyring))
        case "compression+encryption":
            return DataConverter(codec=CodecChain([CompressionCodec(), EncryptionCodec(keyring)]))
        case "compression+encryption+offload":
            return DataConverter(
                codec=CodecChain(
                    [
                        CompressionCodec(),
                        EncryptionCodec(keyring),
                        OffloadCodec(MemoryBlobStore(), threshold_bytes=_OFFLOAD_THRESHOLD),
                    ]
                )
            )
        case _:
            raise ValueError(f"unknown codec {codec_name!r}")


class CodecScenario(Scenario):
    name = "codec_overhead"
    description = "Throughput at different codec chains, for a compressible payload"
    metric = "throughput"
    unit = "workflows/s"

    def cases(self, *, quick: bool) -> Sequence[Case]:
        trials, warmup = (2, 0) if quick else (5, 1)
        runs = 30 if quick else 150
        codecs = (
            ("none", "compression", "encryption")
            if quick
            else (
                "none",
                "compression",
                "encryption",
                "compression+encryption",
                "compression+encryption+offload",
            )
        )
        return [
            Case(
                codec,
                {"runs": runs, "payload_bytes": _PAYLOAD_BYTES, "codec": codec},
                trials=trials,
                warmup=warmup,
            )
            for codec in codecs
        ]

    async def run_trial(self, ctx: BenchContext, case: Case) -> Trial:
        runs = case.params["runs"]
        payload = make_payload(case.params["payload_bytes"])
        converter = _converter(case.params["codec"])
        async with running_app(ctx, converter=converter) as app:
            start = time.perf_counter()
            handles = [await app.start_workflow(simple_workflow, payload) for _ in range(runs)]
            await asyncio.gather(*(h.result(timeout=120) for h in handles))
            elapsed = time.perf_counter() - start
        return Trial(operations=runs, seconds=elapsed)
