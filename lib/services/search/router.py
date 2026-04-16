import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from lib.auth.dependencies import get_current_active_user
from lib.auth.models import User
from lib.core.container import container
from lib.services.datasets.schemas import SearchRequest, SearchResponse, TopSearchResponse

router = APIRouter(tags=["Search"])


@router.post("/search", response_model=SearchResponse)
async def search_datasets(
    body: SearchRequest,
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger),
):
    """Semantic search for datasets using vector similarity."""
    return await container.search_service.search(
        session=db,
        query=body.query,
        filters=body.to_filters(),
        limit=body.limit,
        offset=body.offset,
        user_id=current_user.id,
    )


@router.get("/search/top", response_model=TopSearchResponse)
async def get_top_datasets(
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
):
    """Returns top datasets ranked by popularity score."""
    return await container.search_service.get_top_datasets(session=db, limit=5)


@router.get("/visit/{dataset_id}", response_class=RedirectResponse)
async def visit_dataset(
    dataset_id: UUID,
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger),
):
    """Log the click and redirect user to the original dataset source."""
    dataset = await container.dataset_repo.get_by_id(db, dataset_id)

    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    logger.info(f"User {current_user.id} visiting dataset {dataset_id}")
    return RedirectResponse(url=dataset.url)
