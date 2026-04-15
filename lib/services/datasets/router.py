import time
import logging
from typing import Annotated
from uuid import uuid4
from datetime import datetime
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from lib.core.container import container
from lib.services.datasets.schemas import SearchRequest, SearchResponse, DatasetItem
from lib.auth.dependencies import get_current_active_user
from lib.auth.models import User
from uuid import UUID
from fastapi.responses import RedirectResponse

router = APIRouter(tags=["Datasets"])

@router.post("/search", response_model=SearchResponse)
async def search_datasets(
    body: SearchRequest,
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Semantic search for datasets using RAG-system."""
    start_time = time.perf_counter()

    logger.info(
        f"""Processing search query: '{body.query}',
            limit={body.limit}"""
        )

    mock_items = [
        DatasetItem(
            id=uuid4(),
            source_name="kaggle",
            external_id="user/titanic",
            title="Titanic - Machine Learning from Disaster",
            description="Predict survival on the Titanic...",
            url="https://kaggle.com/c/titanic",
            score=0.98,
            created_at=datetime.now()
        )
    ]

    execution_time = (time.perf_counter() - start_time) * 1000

    return SearchResponse(
        items=mock_items,
        total=len(mock_items),
        execution_time_ms=round(execution_time, 2)
    )

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
