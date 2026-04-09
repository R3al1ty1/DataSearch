from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.models.search_log import SearchLog
from lib.repositories.base import BaseRepository


class SearchLogRepository(BaseRepository[SearchLog]):
    """Repository for search log operations."""

    def __init__(self):
        super().__init__(SearchLog)

    async def log_search(
        self,
        session: AsyncSession,
        user_id: UUID,
        query: str,
        filters: dict | None,
        result_count: int,
        latency_ms: float,
    ) -> None:
        """Logs a search query for analytics."""
        log = SearchLog(
            user_id=user_id,
            query=query,
            filters=filters,
            result_count=result_count,
            latency_ms=latency_ms,
        )
        session.add(log)
        await session.flush()
