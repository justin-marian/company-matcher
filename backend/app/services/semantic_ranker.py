from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from app.services.models import QueryIntent


FIELD_WEIGHTS = {
    "name": 0.30,
    "industry": 0.30,
    "offerings": 0.20,
    "description": 0.15,
    "location": 0.05,
}


def rank_companies(
    df: pd.DataFrame,
    intent: QueryIntent,
    embedder: SentenceTransformer,
    top_k: int,
    semantic_weight: float = 0.70,
    lexical_weight: float = 0.30,
) -> pd.DataFrame:
    """Rank candidates with a blended semantic and weighted lexical score."""

    if df.empty:
        return df.copy()
    if not intent.semantic_query:
        raise ValueError("intent.semantic_query must not be empty")

    ranked = df.copy()
    company_texts: list[str] = []
    field_maps: list[dict[str, str]] = []

    for _, row in ranked.iterrows():
        company_text, field_map = build_company_profile(row)
        company_texts.append(company_text)
        field_maps.append(field_map)

    ranked["company_text"] = company_texts
    ranked["field_map"] = field_maps

    query_vector = embedder.encode([intent.semantic_query], show_progress_bar=False, normalize_embeddings=True)
    company_vectors = embedder.encode(company_texts, batch_size=64, show_progress_bar=False, normalize_embeddings=True)

    raw_embed_score = cosine_similarity(query_vector, company_vectors)[0]
    embed_score = minmax_normalize(raw_embed_score)
    lexical_score = ranked["field_map"].apply(lambda fields: weighted_lexical_score(fields, intent))

    ranked["_embed_score_raw"] = raw_embed_score
    ranked["_embed_score"] = embed_score
    ranked["_lexical_score"] = lexical_score
    ranked["_rank_score"] = semantic_weight * ranked["_embed_score"] + lexical_weight * ranked["_lexical_score"]

    ranked = ranked.sort_values(by=["_rank_score", "_embed_score_raw", "_lexical_score"], ascending=False)
    return ranked.head(top_k).drop(columns=["company_text", "field_map"], errors="ignore").copy()


def build_company_profile(row: pd.Series) -> tuple[str, dict[str, str]]:
    """Build the text used for embeddings and the field map used for lexical scoring."""

    name = safe_text(row.get("operational_name"))
    description = safe_text(row.get("description"))
    location = stringify_address(row.get("address"))
    primary = stringify_naics(row.get("primary_naics"))
    secondary = stringify_naics(row.get("secondary_naics"))
    offerings = stringify_list(row.get("core_offerings"))
    markets = stringify_list(row.get("target_markets"))
    models = stringify_list(row.get("business_model"))

    parts: list[str] = []
    if name:
        parts.append(f"Company: {name}")
    if location:
        parts.append(f"Location: {location}")
    if description:
        parts.append(f"Description: {description}")
    if primary:
        parts.append(f"Primary industry: {primary}")
    if secondary:
        parts.append(f"Secondary industries: {secondary}")
    if offerings:
        parts.append(f"Offerings: {offerings}")
    if markets:
        parts.append(f"Markets: {markets}")
    if models:
        parts.append(f"Business model: {models}")

    fields = {
        "name": name,
        "industry": f"{primary} {secondary}".strip(),
        "offerings": offerings,
        "description": description,
        "location": location,
    }
    return " | ".join(parts), fields


def weighted_lexical_score(fields: dict[str, str], intent: QueryIntent) -> float:
    """Compute a weighted lexical relevance score across the most useful company fields."""

    keywords = unique_keywords(intent)
    score = 0.0
    for field_name, weight in FIELD_WEIGHTS.items():
        text = fields.get(field_name, "")
        coverage = keyword_coverage_score(text, keywords)
        phrase = phrase_bonus(text, intent.semantic_query)
        if field_name == "description":
            phrase *= 0.5
        score += weight * (coverage + phrase)
    return min(score, 1.15)


def unique_keywords(intent: QueryIntent) -> list[str]:
    """Build a compact keyword set from the interpreted query intent."""

    items: list[str] = []
    items.extend(intent.industry_keywords)
    items.extend(intent.business_models)
    items.extend(intent.role_intent.split())
    items.extend(tokenize(intent.semantic_query))
    items.extend(tokenize(intent.raw_query))

    seen: set[str] = set()
    keywords: list[str] = []
    for item in items:
        token = normalize_text(str(item))
        if len(token) < 2 or token in seen:
            continue
        seen.add(token)
        keywords.append(token)
    return keywords


def keyword_coverage_score(text: str, keywords: list[str]) -> float:
    """Measure how many distinct keywords appear in the text, with a small repetition bonus."""

    if not text or not keywords:
        return 0.0

    normalized = normalize_text(text)
    matched = 0
    repetition_bonus = 0.0
    for keyword in keywords:
        count = len(re.findall(rf"\b{re.escape(keyword)}\b", normalized))
        if count > 0:
            matched += 1
            repetition_bonus += min(count - 1, 2) * 0.05

    coverage = matched / len(keywords)
    return min(1.0, coverage + repetition_bonus)


def phrase_bonus(text: str, query_text: str) -> float:
    """Reward exact multi-word query phrase matches inside a field."""

    if not text or not query_text:
        return 0.0
    normalized_text = normalize_text(text)
    normalized_query = normalize_text(query_text)
    if len(normalized_query.split()) < 2:
        return 0.0
    return 0.15 if normalized_query in normalized_text else 0.0


def minmax_normalize(values: np.ndarray) -> np.ndarray:
    """Normalize an array into [0, 1] while keeping constant arrays stable."""

    array = np.asarray(values, dtype=float)
    if len(array) == 0:
        return array
    minimum = np.min(array)
    maximum = np.max(array)
    if np.isclose(minimum, maximum):
        return np.full_like(array, 0.5)
    return (array - minimum) / (maximum - minimum)


def tokenize(text: str) -> list[str]:
    """Tokenize normalized text into non-empty word-like pieces."""

    return [token for token in normalize_text(text).split() if token]


def normalize_text(text: str) -> str:
    """Normalize free text for robust lexical matching."""

    lowered = str(text).lower()
    cleaned = re.sub(r"[^a-z0-9\s\-&/]", " ", lowered)
    return re.sub(r"\s+", " ", cleaned).strip()


def safe_text(value: Any) -> str:
    """Return a stripped string while treating pandas missing values as empty text."""

    return "" if pd.isna(value) else str(value).strip()


def stringify_address(address: Any) -> str:
    """Serialize a structured address into a short human-readable location string."""

    if isinstance(address, dict):
        parts = [address.get("town"), address.get("region_name"), address.get("country_code")]
        return ", ".join(str(part) for part in parts if part)
    return str(address) if address is not None else ""


def stringify_naics(value: Any) -> str:
    """Serialize one or more NAICS entries into a readable label string."""

    if isinstance(value, dict):
        code = value.get("code")
        label = value.get("label")
        if code or label:
            return f"{label} ({code})" if code else str(label or "")
        return ""
    if isinstance(value, list):
        return ", ".join(item for item in (stringify_naics(entry) for entry in value) if item)
    return str(value) if value is not None else ""


def stringify_list(value: Any) -> str:
    """Serialize a list field into a comma-separated string."""

    if isinstance(value, list):
        return ", ".join(str(item) for item in value if str(item).strip())
    return str(value) if value is not None else ""
