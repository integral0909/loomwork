"""Steps for the document pipeline: stand-ins for OCR, a classifier and a document
store. Every step takes ``tenant_id`` first (even where the step itself does not need
it) so the audit-log step interceptor in ``main.py`` can see who touched what -- a
common shape for multi-tenant systems, where the tenant is threaded through everything.
"""

from __future__ import annotations

import logging

import loomwork as lw

logger = logging.getLogger("traced_pipeline")

#: Documents "stored" so far: location -> (tenant_id, label). A stand-in for a real
#: document store.
stored: dict[str, tuple[str, str]] = {}


@lw.step(name="traced_pipeline.extract_text")
async def extract_text(_tenant_id: str, document_id: str) -> str:
    logger.info("extracting text from %s", document_id)
    return f"contents of {document_id} (an invoice)"


@lw.step(name="traced_pipeline.classify_document")
async def classify_document(_tenant_id: str, document_id: str, text: str) -> str:
    logger.info("classifying %s", document_id)
    return "invoice" if "invoice" in text else "general"


@lw.step(name="traced_pipeline.store_result")
async def store_result(tenant_id: str, document_id: str, label: str) -> str:
    location = f"{tenant_id}/{document_id}.json"
    logger.info("storing %s at %s", document_id, location)
    stored[location] = (tenant_id, label)
    return location
