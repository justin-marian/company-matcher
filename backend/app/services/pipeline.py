from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import anthropic
from sentence_transformers import SentenceTransformer

from backend.app.services.company_qualifier import qualify_candidates
from backend.app.services.data_store import initialize_database, load_companies
from backend.app.services.hard_filters import apply_hard_filters, fetch_filtered_candidates
from backend.app.services.models import QualifiedCompany, QueryIntent, RuntimeSettings
from backend.app.services.query_analysis import analyze_query
from backend.app.services.semantic_ranker import rank_companies


class QualificationPipeline:
    """Run query analysis, deterministic filtering, ranking, and final qualification."""

    def __init__(self, settings: RuntimeSettings | None = None):
        """Initialize all long-lived resources required by the pipeline."""

        self.settings = settings or RuntimeSettings()
        self.client = anthropic.Anthropic()
        self.conn: sqlite3.Connection | None = None
        self.df = load_companies(self.settings.data_path)
        self.embedder = SentenceTransformer(self.settings.embed_model)

        if self.settings.use_sql:
            initialize_database(self.df, self.settings.db_path, self.settings.table_name, self.settings.rebuild_db)
            self.conn = sqlite3.connect(self.settings.db_path)
            self.conn.row_factory = sqlite3.Row

    def run(self, query: str, top_k: int | None = None) -> list[QualifiedCompany]:
        """Run one search query end to end and return qualified companies."""

        start_time = time.time()
        intent = analyze_query(self.client, query, self.settings.model, self.settings.prompts_path)
        candidates = self.fetch_candidates(intent)
        if candidates.empty:
            return []

        ranked = rank_companies(candidates, intent, self.embedder, top_k or self.settings.top_k)
        if ranked.empty:
            return []

        results = qualify_candidates(
            candidates=ranked,
            intent=intent,
            model=self.settings.model,
            batch_size=self.settings.batch_size,
            max_concurrent=self.settings.max_concurrent,
            threshold=self.settings.qualify_threshold,
            prompts_path=self.settings.prompts_path,
        )
        results.sort(key=lambda item: (not item.matched, -item.llm_score, -item.embedding_score, -item.lexical_score))
        elapsed = time.time() - start_time
        print(f"Completed query in {elapsed:.1f}s with {sum(item.matched for item in results)}/{len(results)} matches.")
        return results

    def run_queries(self, queries: list[str], top_k: int | None = None) -> dict[str, list[QualifiedCompany]]:
        """Run several queries and return a mapping keyed by the original query string."""

        return {query: self.run(query, top_k=top_k) for query in queries}

    def fetch_candidates(self, intent: QueryIntent):
        """Fetch candidates through SQL when available, otherwise filter the in-memory frame."""

        if self.conn is not None:
            return fetch_filtered_candidates(self.conn, self.settings.table_name, intent)
        return apply_hard_filters(self.df, intent)

    def close(self) -> None:
        """Release open database resources owned by the pipeline."""

        if self.conn is not None:
            self.conn.close()
            self.conn = None


def print_results(query: str, results: list[QualifiedCompany], top_n: int = 10) -> None:
    """Print a compact terminal report for the highest-confidence matched companies."""

    matched = [item for item in results if item.matched]
    unmatched = [item for item in results if not item.matched]

    print(f"\n{'=' * 72}")
    print(f"Query: {query}")
    print(f"Matched: {len(matched)} | Evaluated: {len(results)}")
    print("=" * 72)

    for index, item in enumerate(matched[:top_n], start=1):
        revenue = item.company.get("revenue")
        print(f"\n{index:>2}. {item.name}")
        print(f"    Address   : {item.company.get('address') or 'N/A'}")
        print(f"    Employees : {item.company.get('employee_count') or 'N/A'}")
        print(f"    Revenue   : ${revenue:,.0f}" if revenue else "    Revenue   : N/A")
        print(f"    LLM score : {item.llm_score}/10")
        print(f"    Embed sim : {item.embedding_score:.4f}")
        print(f"    Lexical   : {item.lexical_score:.4f}")
        print(f"    Reason    : {item.reason}")

    if unmatched:
        print(f"\n(+ {len(unmatched)} candidates evaluated but not matched)")


def save_results(results_by_query: dict[str, list[QualifiedCompany]], output_path: Path = Path("results.json")) -> None:
    """Persist matched companies in a compact JSON report grouped by query."""

    payload: dict[str, Any] = {
        query: {
            "matched_count": sum(item.matched for item in results),
            "evaluated_count": len(results),
            "companies": [item.to_dict() for item in results if item.matched],
        }
        for query, results in results_by_query.items()
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
