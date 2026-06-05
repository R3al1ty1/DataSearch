import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse

from lib.auth.dependencies import get_current_active_user, get_uow
from lib.auth.models import User
from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.schemas import ClickRequest, SearchRequest, SearchResponse, TopSearchResponse

router = APIRouter(tags=["Search"])


@router.post("/search", response_model=SearchResponse)
async def search_datasets(
    body: SearchRequest,
    current_user: Annotated[User, Depends(get_current_active_user)],
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger),
):
    """Semantic search for datasets using vector similarity."""
    return await container.search_service.search(
        uow=uow,
        query=body.query,
        filters=body.to_filters(),
        limit=body.limit,
        offset=body.offset,
        user_id=current_user.id,
    )


@router.get("/search/top", response_model=TopSearchResponse)
async def get_top_datasets(
    current_user: Annotated[User, Depends(get_current_active_user)],
    uow: UnitOfWork = Depends(get_uow, scope="function"),
):
    """Returns top datasets ranked by static quality score."""
    return await container.search_service.get_top_datasets(uow=uow, limit=5)


@router.post("/search/click", status_code=status.HTTP_204_NO_CONTENT)
async def record_click(
    body: ClickRequest,
    current_user: Annotated[User, Depends(get_current_active_user)],
    uow: UnitOfWork = Depends(get_uow, scope="function"),
):
    """Records a click event when a user selects a search result."""
    dataset = await uow.datasets.get_by_id(body.dataset_id)
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    await container.search_service.record_click(
        uow=uow,
        user_id=current_user.id,
        dataset_id=body.dataset_id,
        search_log_id=body.search_log_id,
        position=body.position,
    )


@router.get("/visit/{dataset_id}", response_class=RedirectResponse)
async def visit_dataset(
    dataset_id: UUID,
    current_user: Annotated[User, Depends(get_current_active_user)],
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger),
    search_log_id: UUID | None = Query(default=None),
    position: int | None = Query(default=None, ge=0),
):
    """Records a click and redirects to the original dataset source."""
    dataset = await uow.datasets.get_by_id(dataset_id)

    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    if search_log_id is not None and position is not None:
        await container.search_service.record_click(
            uow=uow,
            user_id=current_user.id,
            dataset_id=dataset_id,
            search_log_id=search_log_id,
            position=position,
        )

    logger.info(f"User {current_user.id} visiting dataset {dataset_id}")
    return RedirectResponse(url=dataset.url)
