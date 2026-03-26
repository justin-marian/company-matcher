from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RuntimeSettings(BaseModel):
    """Validated runtime configuration for the retrieval and qualification pipeline."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    model: str = "claude-opus-4-5"
    embed_model: str = "all-MiniLM-L6-v2"
    top_k: int = 25
    batch_size: int = 5
    max_concurrent: int = 5
    qualify_threshold: int = 5
    data_path: Path = Path("data/companies.jsonl")
    prompts_path: Path = Path("prompts")
    use_sql: bool = True
    db_path: Path = Path("data/companies.sqlite")
    table_name: str = "companies"
    rebuild_db: bool = False


class QueryIntent(BaseModel):
    """Structured interpretation of the user's company search intent."""

    raw_query: str
    geo_country: str | None = None
    geo_region: str | None = None
    geo_countries: list[str] = Field(default_factory=list)
    industry_keywords: list[str] = Field(default_factory=list)
    naics_prefixes: list[str] = Field(default_factory=list)
    min_employees: int | None = None
    max_employees: int | None = None
    min_revenue: float | None = None
    max_revenue: float | None = None
    founded_after: int | None = None
    founded_before: int | None = None
    is_public: bool | None = None
    business_models: list[str] = Field(default_factory=list)
    role_intent: str = "core operator"
    semantic_query: str = ""
    complexity: str = "medium"

    def normalized_countries(self) -> list[str]:
        """Return a deduplicated, lowercased country list from all geography fields."""

        seen: set[str] = set()
        countries: list[str] = []
        values = list(self.geo_countries)
        if self.geo_country:
            values.append(self.geo_country)

        for value in values:
            country = str(value).strip().lower()
            if country and country not in seen:
                seen.add(country)
                countries.append(country)
        return countries


class QualifiedCompany(BaseModel):
    """Final company result with ranking signals and qualification outcome."""

    company: dict[str, Any]
    embedding_score: float = 0.0
    lexical_score: float = 0.0
    llm_score: int = 0
    matched: bool = False
    reason: str = ""

    @property
    def name(self) -> str:
        """Return the best display name available for the company."""

        return self.company.get("operational_name") or self.company.get("website") or "Unknown"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the response payload returned by the API."""

        return {
            "operational_name": self.name,
            "website": self.company.get("website"),
            "address": self.company.get("address"),
            "employee_count": self.company.get("employee_count"),
            "revenue": self.company.get("revenue"),
            "description": (self.company.get("description") or "")[:300],
            "embedding_score": round(self.embedding_score, 4),
            "lexical_score": round(self.lexical_score, 4),
            "llm_score": self.llm_score,
            "matched": self.matched,
            "reason": self.reason,
        }
