from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    """API payload for a company search request."""

    query: str = Field(min_length=2, max_length=500)
    top_k: int | None = Field(default=None, ge=1, le=100)
    only_matched: bool = True


class CompanyResult(BaseModel):
    """Serialized company result returned by the search endpoint."""

    operational_name: str
    website: str | None = None
    address: Any = None
    employee_count: float | int | None = None
    revenue: float | int | None = None
    description: str | None = None
    embedding_score: float = 0.0
    lexical_score: float = 0.0
    llm_score: int = 0
    matched: bool = False
    reason: str = ""


class SearchResponse(BaseModel):
    """API response payload for company search results."""

    query: str
    matched_count: int
    evaluated_count: int
    results: list[CompanyResult]


class HealthResponse(BaseModel):
    """Health endpoint payload describing runtime status."""

    status: str
    environment: str
    data_path: str
    sql_enabled: bool
