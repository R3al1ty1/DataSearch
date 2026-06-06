from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.base_repository import BaseRepository
from lib.services.datasets.exceptions import DatasetNotFound, SearchLogNotFound
from lib.services.datasets.models import Dataset, SearchClickEvent, SearchLog


class ClickRepository(BaseRepository[SearchClickEvent]):

    def __init__(self, session: AsyncSession):
        super().__init__(SearchClickEvent, session)

    async def record_click(
        self,
        user_id: UUID | None,
        dataset_id: UUID,
        search_log_id: UUID | None,
        position: int,
    ) -> None:
        if not await self._dataset_exists(dataset_id):
            raise DatasetNotFound(dataset_id)
        if search_log_id is not None and not await self._search_log_exists(
            search_log_id
        ):
            raise SearchLogNotFound(search_log_id)

        event = SearchClickEvent(
            user_id=user_id,
            dataset_id=dataset_id,
            search_log_id=search_log_id,
            position=position,
        )
        self.session.add(event)
        await self.session.flush()

    async def _dataset_exists(self, dataset_id: UUID) -> bool:
        result = await self.session.execute(
            select(Dataset.id).where(Dataset.id == dataset_id)
        )
        return result.scalar_one_or_none() is not None

    async def _search_log_exists(self, search_log_id: UUID) -> bool:
        result = await self.session.execute(
            select(SearchLog.id).where(SearchLog.id == search_log_id)
        )
        return result.scalar_one_or_none() is not None
