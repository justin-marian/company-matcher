from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import anthropic

from backend.app.services.models import QueryIntent


REGION_TO_COUNTRIES: dict[str, list[str]] = {
    "scandinavia": ["Denmark", "Norway", "Sweden"],
    "nordics": ["Denmark", "Norway", "Sweden", "Finland", "Iceland"],
    "europe": [
        "Germany", "France", "Italy", "Spain", "Romania", "Poland", "Netherlands", "Belgium",
        "Switzerland", "Austria", "Sweden", "Norway", "Denmark", "Finland", "Ireland",
        "Portugal", "Czech Republic", "Hungary", "Greece",
    ],
    "united states": ["United States", "USA", "US", "U.S."],
}

INDUSTRY_KEYWORDS = [
    "logistics", "software", "food", "beverage", "construction", "pharmaceutical", "hr",
    "clean energy", "fintech", "e-commerce", "renewable energy", "battery",
]


def analyze_query(client: anthropic.Anthropic, raw_query: str, model: str, prompts_path: Path) -> QueryIntent:
    """Convert a raw query into a validated intent, with a heuristic fallback on failure."""

    prompt = read_prompt_section(prompts_path, "Query Analysis Prompt")
    try:
        response = client.messages.create(
            model=model,
            max_tokens=900,
            messages=[{"role": "user", "content": f"{prompt}\n\nUser query: {raw_query}"}],
        )
        payload = json.loads(clean_json_blob(response.content[0].text))
    except Exception:
        payload = heuristic_intent(raw_query)

    geo_country = payload.get("geo_country")
    geo_region = payload.get("geo_region")
    geo_countries = clean_string_list(payload.get("geo_countries"))

    if not geo_countries and isinstance(geo_region, str):
        geo_countries = REGION_TO_COUNTRIES.get(geo_region.strip().lower(), [])
    if geo_country and geo_country not in geo_countries:
        geo_countries.append(geo_country)

    return QueryIntent(
        raw_query=raw_query,
        geo_country=geo_country,
        geo_region=geo_region,
        geo_countries=geo_countries,
        industry_keywords=clean_string_list(payload.get("industry_keywords")),
        naics_prefixes=clean_string_list(payload.get("naics_prefixes")),
        min_employees=payload.get("min_employees"),
        max_employees=payload.get("max_employees"),
        min_revenue=payload.get("min_revenue"),
        max_revenue=payload.get("max_revenue"),
        founded_after=payload.get("founded_after"),
        founded_before=payload.get("founded_before"),
        is_public=payload.get("is_public"),
        business_models=clean_string_list(payload.get("business_models")),
        role_intent=(payload.get("role_intent") or "core operator").strip(),
        semantic_query=(payload.get("semantic_query") or raw_query).strip(),
        complexity=(payload.get("complexity") or "medium").strip().lower(),
    )


def heuristic_intent(raw_query: str) -> dict[str, Any]:
    """Derive a conservative intent when the LLM response is unavailable or malformed."""

    query = raw_query.lower()
    geo_country: str | None = None
    geo_countries: list[str] = []

    for region, countries in REGION_TO_COUNTRIES.items():
        if region in query:
            geo_countries.extend(countries)
            if region == "united states":
                geo_country = "United States"

    if any(token in query for token in ["supplier", "supply", "components", "packaging"]):
        role_intent = "supplier"
    elif any(token in query for token in ["software", "saas", "platform"]):
        role_intent = "software"
    elif any(token in query for token in ["competing with", "adjacent"]):
        role_intent = "adjacent"
    else:
        role_intent = "core operator"

    complexity = "high" if any(token in query for token in ["supplier", "competing", "critical components"]) else "medium"
    business_models = ["B2B"] if "b2b" in query else ["B2C"] if "b2c" in query else []

    return {
        "geo_country": geo_country,
        "geo_countries": geo_countries,
        "geo_region": None,
        "industry_keywords": [keyword for keyword in INDUSTRY_KEYWORDS if keyword in query],
        "naics_prefixes": [],
        "min_employees": extract_int(query, [r"more than (\d[\d,]*) employees", r"over (\d[\d,]*) employees"]),
        "max_employees": extract_int(query, [r"fewer than (\d[\d,]*) employees", r"under (\d[\d,]*) employees"]),
        "min_revenue": extract_money(query, [r"revenue over \$?(\d[\d,\.]*)\s*(million|billion)?"]),
        "max_revenue": None,
        "founded_after": extract_int(query, [r"founded after (\d{4})", r"after (\d{4})"]),
        "founded_before": None,
        "is_public": True if "public" in query else None,
        "business_models": business_models,
        "role_intent": role_intent,
        "semantic_query": raw_query,
        "complexity": complexity,
    }


def read_prompt_section(prompts_path: Path, heading: str) -> str:
    """Load one H1 section from a prompt file or prompt directory."""

    if prompts_path.is_dir():
        markdown_files = sorted(prompts_path.glob("*.md"))
        if not markdown_files:
            raise FileNotFoundError(f"No .md files found in prompts directory: {prompts_path}")
        text = "\n\n".join(path.read_text(encoding="utf-8") for path in markdown_files)
    else:
        text = prompts_path.read_text(encoding="utf-8")

    pattern = rf"^# {re.escape(heading)}\s*\n(.*?)(?=^# |\Z)"
    match = re.search(pattern, text, flags=re.MULTILINE | re.DOTALL)
    if not match:
        raise ValueError(f"Prompt section '{heading}' not found in {prompts_path}")
    return match.group(1).strip()


def clean_json_blob(text: str) -> str:
    """Strip code fences and keep the first JSON object in the model response."""

    stripped = re.sub(r"^```json\s*|^```|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    return stripped[start:end + 1] if start != -1 and end >= start else stripped


def clean_string_list(value: Any) -> list[str]:
    """Normalize a list of user-facing strings while preserving insertion order."""

    if not isinstance(value, list):
        return []

    seen: set[str] = set()
    items: list[str] = []
    for raw_item in value:
        item = str(raw_item).strip()
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            items.append(item)
    return items


def extract_int(text: str, patterns: list[str]) -> int | None:
    """Extract the first integer matched by the provided regex patterns."""

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1).replace(",", ""))
    return None


def extract_money(text: str, patterns: list[str]) -> float | None:
    """Extract a revenue amount and scale it when the query uses million or billion."""

    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        value = float(match.group(1).replace(",", ""))
        unit = (match.group(2) or "").lower()
        if unit == "million":
            value *= 1_000_000
        elif unit == "billion":
            value *= 1_000_000_000
        return value
    return None
