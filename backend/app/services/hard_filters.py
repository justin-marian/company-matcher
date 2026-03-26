from __future__ import annotations

import json
import sqlite3
from typing import Any

import pandas as pd

from app.services.models import QueryIntent


COUNTRY_CODE_ALIASES = {
    "romania": {"ro", "rou"},
    "france": {"fr", "fra"},
    "germany": {"de", "deu"},
    "switzerland": {"ch", "che"},
    "united states": {"us", "usa"},
    "norway": {"no", "nor"},
    "sweden": {"se", "swe"},
    "denmark": {"dk", "dnk"},
    "finland": {"fi", "fin"},
    "poland": {"pl", "pol"},
    "bulgaria": {"bg", "bgr"},
    "georgia": {"ge", "geo"},
    "moldova": {"md", "mda"},
}


def apply_hard_filters(df: pd.DataFrame, intent: QueryIntent) -> pd.DataFrame:
    """Apply deterministic filters in memory when SQL retrieval is disabled."""

    mask = pd.Series(True, index=df.index)
    countries = intent.normalized_countries()

    if countries:
        mask &= df["address"].apply(lambda address: address_matches_geo(address, countries))
    if intent.naics_prefixes:
        mask &= df.apply(lambda row: naics_matches(row.get("primary_naics"), row.get("secondary_naics"), intent.naics_prefixes), axis=1)

    employee_count = pd.to_numeric(safe_series(df, "employee_count"), errors="coerce")
    revenue = pd.to_numeric(safe_series(df, "revenue"), errors="coerce")
    founded_year = pd.to_numeric(safe_series(df, "year_founded"), errors="coerce")

    if intent.min_employees is not None:
        mask &= employee_count.isna() | (employee_count >= intent.min_employees)
    if intent.max_employees is not None:
        mask &= employee_count.isna() | (employee_count <= intent.max_employees)
    if intent.min_revenue is not None:
        mask &= revenue.isna() | (revenue >= intent.min_revenue)
    if intent.max_revenue is not None:
        mask &= revenue.isna() | (revenue <= intent.max_revenue)
    if intent.founded_after is not None:
        mask &= founded_year.isna() | (founded_year >= intent.founded_after)
    if intent.founded_before is not None:
        mask &= founded_year.isna() | (founded_year <= intent.founded_before)
    if intent.is_public is not None:
        is_public = safe_series(df, "is_public")
        mask &= is_public.isna() | (is_public == intent.is_public)
    if intent.business_models:
        mask &= safe_series(df, "business_model").apply(lambda value: business_model_matches(value, intent.business_models))

    return df[mask].copy()


def fetch_filtered_candidates(conn: sqlite3.Connection, table_name: str, intent: QueryIntent) -> pd.DataFrame:
    """Fetch filtered candidates directly from SQLite to avoid scanning the full dataset."""

    sql, params = build_sql_filter_query(table_name, intent)
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([json.loads(row["payload_json"]) for row in rows])


def build_sql_filter_query(table_name: str, intent: QueryIntent) -> tuple[str, list[Any]]:
    """Build the parameterized SQL query that mirrors the in-memory filter logic."""

    clauses: list[str] = ["1=1"]
    params: list[Any] = []

    countries = intent.normalized_countries()
    if countries:
        geo_clauses: list[str] = []
        for country in countries:
            aliases = sorted(COUNTRY_CODE_ALIASES.get(country, set()))
            country_clauses: list[str] = []
            if aliases:
                placeholders = ", ".join("?" for _ in aliases)
                country_clauses.append(f"country_code IN ({placeholders})")
                params.extend(aliases)
            country_clauses.append("lower(coalesce(region_name, '')) LIKE ?")
            params.append(f"%{country}%")
            country_clauses.append("lower(coalesce(town, '')) LIKE ?")
            params.append(f"%{country}%")
            geo_clauses.append("(" + " OR ".join(country_clauses) + ")")
        clauses.append("(" + " OR ".join(geo_clauses) + ")")

    if intent.naics_prefixes:
        naics_clauses: list[str] = []
        for prefix in intent.naics_prefixes:
            naics_clauses.append("(coalesce(primary_naics_code, '') LIKE ? OR coalesce(secondary_naics_codes, '') LIKE ?)")
            params.extend([f"{prefix}%", f"%{prefix}%"])
        clauses.append("(" + " OR ".join(naics_clauses) + ")")

    add_numeric_clause(clauses, params, "employee_count", intent.min_employees, intent.max_employees)
    add_numeric_clause(clauses, params, "revenue", intent.min_revenue, intent.max_revenue)
    add_numeric_clause(clauses, params, "year_founded", intent.founded_after, intent.founded_before)

    if intent.is_public is not None:
        clauses.append("(is_public IS NULL OR is_public = ?)")
        params.append(int(intent.is_public))

    if intent.business_models:
        model_clauses: list[str] = []
        for model in intent.business_models:
            model_clauses.append("lower(coalesce(business_model_text, '')) LIKE ?")
            params.append(f"%{str(model).strip().lower()}%")
        clauses.append("(" + " OR ".join(model_clauses) + ")")

    sql = f"SELECT payload_json FROM {table_name} WHERE " + " AND ".join(clauses)
    return sql, params


def address_matches_geo(address: Any, countries: list[str]) -> bool:
    """Return True when the address matches any requested country or country alias."""

    if not address or not countries:
        return True
    if isinstance(address, dict):
        country_code = str(address.get("country_code", "")).strip().lower()
        region_name = str(address.get("region_name") or "").strip().lower()
        town = str(address.get("town") or "").strip().lower()
        for country in countries:
            if country in region_name or country in town:
                return True
            if country_code and country_code in COUNTRY_CODE_ALIASES.get(country, set()):
                return True
        return False

    text = str(address).lower()
    return any(country in text for country in countries)


def naics_matches(primary: Any, secondary: Any, prefixes: list[str]) -> bool:
    """Return True when any primary or secondary NAICS code matches a requested prefix."""

    if not prefixes:
        return True
    codes = collect_naics_codes(primary, secondary)
    if not codes:
        return True
    return any(code.startswith(prefix) for code in codes for prefix in prefixes)


def business_model_matches(value: Any, requested: list[str]) -> bool:
    """Return True when actual and requested business models overlap."""

    if not requested or value is None:
        return True
    desired = {item.strip().lower() for item in requested}
    values = value if isinstance(value, list) else [value]
    actual = {str(item).strip().lower() for item in values if str(item).strip()}
    return not actual or bool(desired & actual)


def safe_series(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a column when present, otherwise a same-length nullable fallback series."""

    if column in df.columns:
        return df[column]
    return pd.Series([None] * len(df), index=df.index)


def collect_naics_codes(primary: Any, secondary: Any) -> list[str]:
    """Collect all available primary and secondary NAICS codes as strings."""

    codes: list[str] = []
    if isinstance(primary, dict) and primary.get("code") is not None:
        codes.append(str(primary["code"]))
    if isinstance(secondary, list):
        codes.extend(str(item["code"]) for item in secondary if isinstance(item, dict) and item.get("code") is not None)
    return codes


def add_numeric_clause(clauses: list[str], params: list[Any], column: str, minimum: Any, maximum: Any) -> None:
    """Append nullable lower and upper bound filters for one numeric SQL column."""

    if minimum is not None:
        clauses.append(f"({column} IS NULL OR {column} >= ?)")
        params.append(minimum)
    if maximum is not None:
        clauses.append(f"({column} IS NULL OR {column} <= ?)")
        params.append(maximum)
