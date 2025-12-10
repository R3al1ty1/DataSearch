import logging
from uuid import UUID
from fastapi import APIRouter, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container

router = APIRouter(tags=["Tracking"])


@router.get("/visit/{dataset_id}", response_class=RedirectResponse)
async def visit_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(container.db.get_session_generator),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Log the click and redirect user to the original source."""
    logger.info(f"User clicking on dataset: {dataset_id}")

    # TODO:
    # 1. dataset = await dataset_repo.get_by_id(db, dataset_id)
    # 2. if not dataset: raise 404
    # 3. await tracking_repo.log_click(dataset_id)
    # 4. return RedirectResponse(dataset.url)

    # Mock Logic:
    # Эмулируем ошибку, если ID не найден (в реальности тут будет запрос в БД)
    # raise HTTPException(status_code=404, detail="Dataset not found")

    # Mock Success:
    return RedirectResponse(url="https://www.kaggle.com/")
