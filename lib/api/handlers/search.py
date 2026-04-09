import logging
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from lib.api.dependencies.auth import get_current_active_user
from lib.core.container import container
from lib.models.user import User
from lib.schemas.dataset import SearchRequest, SearchResponse, TopSearchResponse

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
    """Returns top 5 datasets ranked by popularity score."""
    return await container.search_service.get_top_datasets(session=db, limit=5)
