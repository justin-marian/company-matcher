from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Central application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Company Matcher API"
    app_env: str = "development"
    api_prefix: str = "/api"
    allowed_origins: list[str] = Field(default_factory=lambda: [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ])

    anthropic_api_key: str | None = None
    anthropic_model: str = "claude-opus-4-5"
    embed_model: str = "all-MiniLM-L6-v2"

    data_path: Path = Path("data/companies.jsonl")
    prompts_path: Path = Path("prompts")
    db_path: Path = Path("data/companies.sqlite")
    table_name: str = "companies"

    top_k: int = 25
    batch_size: int = 5
    max_concurrent: int = 5
    qualify_threshold: int = 5
    use_sql: bool = True
    rebuild_db: bool = False


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return a cached settings object so the app is configured once per process."""

    return AppSettings()
