import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container

router = APIRouter(tags=["Tracking"])


@router.get("/visit/{dataset_id}", response_class=RedirectResponse)
async def visit_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger),
):
    """Log the click and redirect user to the original source."""
    dataset = await container.dataset_repo.get_by_id(db, dataset_id)

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found",
        )

    logger.info(f"Visiting dataset {dataset_id} → {dataset.url}")

    return RedirectResponse(url=dataset.url)
