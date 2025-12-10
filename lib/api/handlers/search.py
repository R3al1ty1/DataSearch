import time
import logging
from uuid import uuid4
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container
from lib.schemas.dataset import SearchRequest, SearchResponse, DatasetItem

router = APIRouter(tags=["Search"])


@router.post("/search", response_model=SearchResponse)
async def search_datasets(
    body: SearchRequest,
    db: AsyncSession = Depends(container.db.get_session_generator),
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
