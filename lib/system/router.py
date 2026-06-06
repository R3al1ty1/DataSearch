import logging
from typing import Annotated
from fastapi import APIRouter, Depends
from sqlalchemy import text
from pydantic import BaseModel, Field

from lib.core.container import container
from lib.core.constants import UserRole
from lib.system.schemas import HealthResponse
from lib.system.exceptions import TaskQueueError
from lib.auth.dependencies import get_uow, require_role
from lib.auth.models import User
from lib.core.openapi import COMMON_ERROR_RESPONSES
from lib.core.uow import UnitOfWork

router = APIRouter(tags=["System"], responses=COMMON_ERROR_RESPONSES)


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
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Performs a health check:"""
    await uow.session.execute(text("SELECT 1"))

    logger.info("Health check passed.")

    return HealthResponse(
        status="active",
        environment=container.settings.ENVIRONMENT
    )


@router.post("/tasks/generate-embeddings", response_model=TaskTriggerResponse)
async def trigger_embedding_generation(
    current_user: Annotated[User, Depends(require_role(UserRole.ADMIN))],
    request: EmbeddingTaskRequest = EmbeddingTaskRequest(),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """
    Trigger embedding generation task manually.

    Requires admin role.

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
        raise TaskQueueError("enrich.generate_embeddings", str(e)) from e
