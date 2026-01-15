import logging
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field

from lib.core.container import container
from lib.schemas.common import HealthResponse

router = APIRouter(tags=["System"])


class TaskTriggerResponse(BaseModel):
    """Response for task trigger endpoint."""
    task_name: str
    status: str
    message: str


class EmbeddingTaskRequest(BaseModel):
    """Request for embedding generation task."""
    batch_size: int = Field(default=100, ge=1, le=1000)


@router.get("/health", response_model=HealthResponse)
async def health_check(
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Performs a health check:"""
    await db.execute(text("SELECT 1"))

    logger.info("Health check passed.")

    return HealthResponse(
        status="active",
        environment=container.settings.ENVIRONMENT
    )


@router.post("/tasks/generate-embeddings", response_model=TaskTriggerResponse)
async def trigger_embedding_generation(
    request: EmbeddingTaskRequest = EmbeddingTaskRequest(),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """
    Trigger embedding generation task manually.

    This endpoint queues a Celery task to generate embeddings for datasets
    that don't have them yet (status=ENRICHED, embedding=NULL).
    """
    from lib.crons.enrich import generate_embeddings

    try:
        result = generate_embeddings.delay(request.batch_size)
        logger.info(f"Triggered embedding generation task: {result.id}")

        return TaskTriggerResponse(
            task_name="enrich.generate_embeddings",
            status="queued",
            message=f"Task queued with ID: {result.id}"
        )
    except Exception as e:
        logger.error(f"Failed to trigger embedding generation: {e}")
        return TaskTriggerResponse(
            task_name="enrich.generate_embeddings",
            status="error",
            message=str(e)
        )
