import logging
from typing import Annotated
from uuid import UUID
from fastapi import APIRouter, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container
from lib.api.dependencies.auth import get_current_active_user
from lib.models.user import User

router = APIRouter(tags=["Tracking"])


@router.get("/visit/{dataset_id}", response_class=RedirectResponse)
async def visit_dataset(
    dataset_id: UUID,
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
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
