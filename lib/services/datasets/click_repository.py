from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.base_repository import BaseRepository
from lib.services.datasets.models import SearchClickEvent


class ClickRepository(BaseRepository[SearchClickEvent]):

    def __init__(self):
        super().__init__(SearchClickEvent)

    async def record_click(
        self,
        session: AsyncSession,
        user_id: UUID | None,
        dataset_id: UUID,
        search_log_id: UUID | None,
        position: int,
    ) -> None:
        event = SearchClickEvent(
            user_id=user_id,
            dataset_id=dataset_id,
            search_log_id=search_log_id,
            position=position,
        )
        session.add(event)
        await session.flush()
