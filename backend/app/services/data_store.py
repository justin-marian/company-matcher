from __future__ import annotations

import ast
import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = {
    "website": None,
    "operational_name": None,
    "year_founded": None,
    "address": None,
    "employee_count": None,
    "revenue": None,
    "primary_naics": None,
    "secondary_naics": None,
    "description": None,
    "business_model": None,
    "target_markets": None,
    "core_offerings": None,
    "is_public": None,
}

STRUCTURED_COLUMNS = ("address", "primary_naics", "secondary_naics", "business_model", "target_markets", "core_offerings")
NUMERIC_COLUMNS = ("year_founded", "employee_count", "revenue")


def load_companies(data_path: Path) -> pd.DataFrame:
    """Load raw company records from JSONL and normalize them for downstream use."""

    frame = pd.read_json(data_path, lines=True)
    return normalize_company_frame(frame)


def normalize_company_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the company frame into consistent Python and SQL-friendly types."""

    frame = df.copy()
    for column, default in REQUIRED_COLUMNS.items():
        if column not in frame.columns:
            frame[column] = default

    for column in STRUCTURED_COLUMNS:
        frame[column] = frame[column].apply(parse_field)

    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["is_public"] = frame["is_public"].apply(parse_nullable_bool)
    frame["country_code"] = frame["address"].apply(lambda value: extract_address_part(value, "country_code"))
    frame["region_name"] = frame["address"].apply(lambda value: extract_address_part(value, "region_name"))
    frame["town"] = frame["address"].apply(lambda value: extract_address_part(value, "town"))
    frame["primary_naics_code"] = frame["primary_naics"].apply(extract_primary_naics_code)
    frame["secondary_naics_codes"] = frame["secondary_naics"].apply(join_secondary_naics_codes)
    frame["business_model_text"] = frame["business_model"].apply(join_text_values)
    return frame


def parse_field(value: Any) -> Any:
    """Parse dict-like and list-like string fields without altering normal scalar values."""

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (dict, list, bool, int, float)):
        return value
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return ast.literal_eval(text)
    except Exception:
        return value


def parse_nullable_bool(value: Any) -> bool | None:
    """Convert truthy values to bool while preserving missing values as None."""

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return bool(value)


def extract_address_part(address: Any, key: str) -> str | None:
    """Extract and normalize a single address field from a structured address object."""

    if not isinstance(address, dict):
        return None
    value = address.get(key)
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def extract_primary_naics_code(value: Any) -> str | None:
    """Extract the primary NAICS code if present."""

    if not isinstance(value, dict) or value.get("code") is None:
        return None
    code = str(value["code"]).strip()
    return code or None


def join_secondary_naics_codes(value: Any) -> str:
    """Flatten secondary NAICS codes into a searchable pipe-delimited string."""

    if not isinstance(value, list):
        return ""
    codes = [str(item["code"]).strip() for item in value if isinstance(item, dict) and item.get("code") is not None]
    return "|".join(code for code in codes if code)


def join_text_values(value: Any) -> str:
    """Flatten lists or scalars into a normalized lowercase text field."""

    if isinstance(value, list):
        items = [str(item).strip().lower() for item in value if str(item).strip()]
        return "|".join(items)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().lower()


def initialize_database(df: pd.DataFrame, db_path: Path, table_name: str, rebuild: bool = False) -> None:
    """Materialize the normalized dataset into a SQLite table with filter indexes."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if rebuild and db_path.exists():
        db_path.unlink()

    with sqlite3.connect(db_path) as conn:
        create_table(conn, table_name)
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if row_count and not rebuild:
            ensure_indexes(conn, table_name)
            return
        if row_count:
            conn.execute(f"DELETE FROM {table_name}")

        rows = [build_sql_row(record) for record in df.to_dict("records")]
        conn.executemany(
            f"""
            INSERT INTO {table_name} (
                website, operational_name, year_founded, employee_count, revenue,
                is_public, country_code, region_name, town,
                primary_naics_code, secondary_naics_codes, business_model_text,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        ensure_indexes(conn, table_name)
        conn.commit()


def create_table(conn: sqlite3.Connection, table_name: str) -> None:
    """Create the companies table if it does not already exist."""

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            website TEXT,
            operational_name TEXT,
            year_founded REAL,
            employee_count REAL,
            revenue REAL,
            is_public INTEGER,
            country_code TEXT,
            region_name TEXT,
            town TEXT,
            primary_naics_code TEXT,
            secondary_naics_codes TEXT,
            business_model_text TEXT,
            payload_json TEXT NOT NULL
        )
        """
    )


def ensure_indexes(conn: sqlite3.Connection, table_name: str) -> None:
    """Create the indexes used by deterministic SQL filtering."""

    conn.executescript(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_country_code ON {table_name}(country_code);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_region_name ON {table_name}(region_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_town ON {table_name}(town);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_year_founded ON {table_name}(year_founded);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_employee_count ON {table_name}(employee_count);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_revenue ON {table_name}(revenue);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_is_public ON {table_name}(is_public);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_primary_naics_code ON {table_name}(primary_naics_code);
        """
    )


def build_sql_row(record: dict[str, Any]) -> tuple[Any, ...]:
    """Convert a normalized record into a SQLite insert row."""

    return (
        record.get("website"),
        record.get("operational_name"),
        record.get("year_founded"),
        record.get("employee_count"),
        record.get("revenue"),
        None if record.get("is_public") is None else int(bool(record["is_public"])),
        record.get("country_code"),
        record.get("region_name"),
        record.get("town"),
        record.get("primary_naics_code"),
        record.get("secondary_naics_codes"),
        record.get("business_model_text"),
        build_payload_json(record),
    )


def build_payload_json(record: dict[str, Any]) -> str:
    """Serialize a record while preserving JSON nulls for missing values."""

    payload = {key: to_json_safe_value(value) for key, value in record.items() if not key.startswith("_")}
    return json.dumps(payload, ensure_ascii=False)


def to_json_safe_value(value: Any) -> Any:
    """Convert pandas missing values to JSON null and pass everything else through."""

    if isinstance(value, float) and pd.isna(value):
        return None
    return value
