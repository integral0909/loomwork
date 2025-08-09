"""Pydantic models for the bulk import example."""

from __future__ import annotations

from pydantic import BaseModel


class Page(BaseModel):
    """One page of the dataset: a contiguous slice of record ids."""

    page_id: int
    record_ids: list[int]


class PageResult(BaseModel):
    page_id: int
    imported: int


class ImportSummary(BaseModel):
    total_records: int
    pages_processed: int
    imported: int
    runs: int
