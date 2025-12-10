import logging
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container
from lib.schemas.common import HealthResponse

router = APIRouter(tags=["System"])


@router.get("/health", response_model=HealthResponse)
async def health_check(
    db: AsyncSession = Depends(container.db.get_session_generator),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Performs a health check:"""
    await db.execute(text("SELECT 1"))

    logger.info("Health check passed.")

    return HealthResponse(
        status="active",
        environment=container.settings.ENVIRONMENT
    )
