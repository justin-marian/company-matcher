from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import anthropic
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.app.config import get_settings
from backend.app.deps import close_pipeline, get_pipeline
from backend.app.schemas import CompanyResult, HealthResponse, SearchRequest, SearchResponse


settings = get_settings()
app_dir = Path(__file__).resolve().parent
static_dir = app_dir / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure environment-backed clients on startup and clean resources on shutdown."""

    if settings.anthropic_api_key:
        os.environ["ANTHROPIC_API_KEY"] = settings.anthropic_api_key
    yield
    close_pipeline()


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.app_env != "production" else None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Report whether the API is alive and which backing mode it is using."""

    return HealthResponse(
        status="ok",
        environment=settings.app_env,
        data_path=str(settings.data_path),
        sql_enabled=settings.use_sql,
    )


@app.post(f"{settings.api_prefix}/search", response_model=SearchResponse)
def search_companies(payload: SearchRequest) -> SearchResponse:
    """Run the company matching pipeline and return ranked, optionally filtered results."""

    try:
        pipeline = get_pipeline()
        results = pipeline.run(payload.query, top_k=payload.top_k)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=f"Missing runtime file: {exc}") from exc
    except anthropic.APIError as exc:
        raise HTTPException(status_code=502, detail=f"Anthropic API error: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    visible_results = [item for item in results if item.matched] if payload.only_matched else results
    return SearchResponse(
        query=payload.query,
        matched_count=sum(item.matched for item in results),
        evaluated_count=len(results),
        results=[CompanyResult(**item.to_dict()) for item in visible_results],
    )


if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    def serve_index() -> FileResponse:
        """Serve the static frontend when the backend hosts the UI directly."""

        return FileResponse(str(static_dir / "index.html"))
