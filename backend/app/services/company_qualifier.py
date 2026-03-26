from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Any

import anthropic
import pandas as pd

from backend.app.services.models import QualifiedCompany, QueryIntent
from backend.app.services.query_analysis import read_prompt_section


async def qualify_all(
    candidates: pd.DataFrame,
    intent: QueryIntent,
    model: str,
    batch_size: int,
    max_concurrent: int,
    threshold: int,
    prompts_path: Path,
) -> list[QualifiedCompany]:
    """Qualify ranked candidates concurrently while keeping request pressure bounded."""

    client = anthropic.AsyncAnthropic()
    semaphore = asyncio.Semaphore(max_concurrent)
    system_prompt = read_prompt_section(prompts_path, "Company Qualification Prompt")
    records = candidates.to_dict("records")
    batches = [records[index:index + batch_size] for index in range(0, len(records), batch_size)]

    tasks = [
        qualify_batch(client, model, system_prompt, intent.raw_query, threshold, batch, semaphore)
        for batch in batches
    ]
    outputs = await asyncio.gather(*tasks)

    results: list[QualifiedCompany] = []
    for batch, batch_output in zip(batches, outputs):
        for company, result in zip(batch, batch_output):
            llm_score = int(result.get("score", 0))
            results.append(
                QualifiedCompany(
                    company=company,
                    embedding_score=float(company.get("_embed_score", 0.0)),
                    lexical_score=float(company.get("_lexical_score", 0.0)),
                    llm_score=llm_score,
                    matched=bool(result.get("matched", llm_score >= threshold)),
                    reason=str(result.get("reason", "")).strip(),
                )
            )
    return results


async def qualify_batch(
    client: anthropic.AsyncAnthropic,
    model: str,
    system_prompt: str,
    query: str,
    threshold: int,
    batch: list[dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Qualify one batch with the LLM and fall back deterministically on malformed output."""

    async with semaphore:
        try:
            response = await client.messages.create(
                model=model,
                max_tokens=1200,
                system=system_prompt,
                messages=[{"role": "user", "content": build_prompt(query, threshold, batch)}],
            )
            payload = json.loads(clean_json_array(response.content[0].text))
            if valid_response_shape(payload, len(batch)):
                return payload
        except Exception:
            pass
    return fallback_results(batch, threshold)


def qualify_candidates(
    candidates: pd.DataFrame,
    intent: QueryIntent,
    model: str,
    batch_size: int,
    max_concurrent: int,
    threshold: int,
    prompts_path: Path,
) -> list[QualifiedCompany]:
    """Synchronously qualify candidates by wrapping the async batch pipeline."""

    return asyncio.run(
        qualify_all(
            candidates=candidates,
            intent=intent,
            model=model,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            threshold=threshold,
            prompts_path=prompts_path,
        )
    )


def build_prompt(query: str, threshold: int, companies: list[dict[str, Any]]) -> str:
    """Build the user prompt sent to the LLM for one qualification batch."""

    payload = [compact_company_payload(company) for company in companies]
    companies_json = json.dumps(payload, indent=2, ensure_ascii=False)
    return f'Query: "{query}"\nThreshold: {threshold}\n\nCompanies:\n{companies_json}'


def compact_company_payload(company: dict[str, Any]) -> dict[str, Any]:
    """Keep only the company fields that materially influence qualification quality."""

    allowed_fields = [
        "website", "operational_name", "year_founded", "address", "employee_count", "revenue",
        "primary_naics", "secondary_naics", "description", "business_model", "target_markets",
        "core_offerings", "is_public", "_embed_score", "_lexical_score", "_rank_score",
    ]
    payload = {field: company.get(field) for field in allowed_fields if company.get(field) is not None}
    if "description" in payload:
        payload["description"] = str(payload["description"])[:700]
    return payload


def valid_response_shape(data: Any, batch_len: int) -> bool:
    """Validate that the model returned one result per company with the expected keys."""

    if not isinstance(data, list) or len(data) != batch_len:
        return False
    required_keys = {"score", "matched", "reason"}
    return all(isinstance(item, dict) and required_keys <= item.keys() for item in data)


def fallback_results(batch: list[dict[str, Any]], threshold: int) -> list[dict[str, Any]]:
    """Approximate qualification from semantic and lexical scores when the LLM is unavailable."""

    results: list[dict[str, Any]] = []
    for company in batch:
        embed_score = float(company.get("_embed_score", 0.0))
        lexical_score = float(company.get("_lexical_score", 0.0))
        score = min(10, max(0, round(10 * (0.75 * embed_score + 0.25 * lexical_score))))
        results.append({
            "score": score,
            "matched": score >= threshold,
            "reason": "Fallback qualification based on semantic and lexical relevance.",
        })
    return results


def clean_json_array(text: str) -> str:
    """Strip code fences and keep the first JSON array from the model response."""

    stripped = re.sub(r"^```json\s*|^```|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()
    start = stripped.find("[")
    end = stripped.rfind("]")
    return stripped[start:end + 1] if start != -1 and end >= start else stripped
