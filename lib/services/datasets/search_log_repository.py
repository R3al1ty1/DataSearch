from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.base_repository import BaseRepository
from lib.services.datasets.models import SearchLog


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
        result_ids: list[str] | None = None,
        score_version: str = "v1_hybrid",
    ) -> SearchLog:
        """Logs a search query and returns the created SearchLog."""
        log = SearchLog(
            user_id=user_id,
            query=query,
            filters=filters,
            result_count=result_count,
            latency_ms=latency_ms,
            result_ids=result_ids,
            score_version=score_version,
        )
        session.add(log)
        await session.flush()
        return log
