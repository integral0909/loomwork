"""A three-step document pipeline: extract, classify, store.

Nothing here is special-cased for tracing -- the workflow just calls three ordinary
steps. Trace propagation, the audit-log step interceptor and log correlation (see
``main.py``) all attach from the outside, through the client and worker configuration,
exactly as ``docs/guides/interceptors.md`` describes.
"""

from __future__ import annotations

import logging

import loomwork as lw
from examples.traced_pipeline.steps import classify_document, extract_text, store_result

logger = logging.getLogger("traced_pipeline")


@lw.workflow(name="traced_pipeline.process_document")
async def process_document(tenant_id: str, document_id: str) -> dict[str, str]:
    logger.info("processing %s for %s", document_id, tenant_id)
    text = await extract_text(tenant_id, document_id)
    label = await classify_document(tenant_id, document_id, text)
    location = await store_result(tenant_id, document_id, label)
    return {"document_id": document_id, "label": label, "location": location}
