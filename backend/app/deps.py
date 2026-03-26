from __future__ import annotations

import threading

from app.config import get_settings
from app.services.models import RuntimeSettings
from app.services.pipeline import QualificationPipeline


pipeline_instance: QualificationPipeline | None = None
pipeline_lock = threading.Lock()


def get_pipeline() -> QualificationPipeline:
    """Return a lazily created singleton pipeline shared across requests."""

    global pipeline_instance
    if pipeline_instance is None:
        with pipeline_lock:
            if pipeline_instance is None:
                settings = get_settings()
                runtime = RuntimeSettings(
                    model=settings.anthropic_model,
                    embed_model=settings.embed_model,
                    top_k=settings.top_k,
                    batch_size=settings.batch_size,
                    max_concurrent=settings.max_concurrent,
                    qualify_threshold=settings.qualify_threshold,
                    data_path=settings.data_path,
                    prompts_path=settings.prompts_path,
                    use_sql=settings.use_sql,
                    db_path=settings.db_path,
                    table_name=settings.table_name,
                    rebuild_db=settings.rebuild_db,
                )
                pipeline_instance = QualificationPipeline(runtime)
    return pipeline_instance


def close_pipeline() -> None:
    """Close and clear the shared pipeline instance during application shutdown."""

    global pipeline_instance
    if pipeline_instance is not None:
        pipeline_instance.close()
        pipeline_instance = None
