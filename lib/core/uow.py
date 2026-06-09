from __future__ import annotations

from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from lib.auth.repository import SecurityEventRepository, UserRepository
from lib.services.datasets.click_repository import ClickRepository
from lib.services.datasets.repository import DatasetRepository, EnrichmentLogRepository
from lib.services.datasets.search_log_repository import SearchLogRepository


class UnitOfWork:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.session: AsyncSession | None = None

    async def __aenter__(self) -> UnitOfWork:
        self.session = self._session_factory()
        self.users = UserRepository(self.session)
        self.security_events = SecurityEventRepository(self.session)
        self.datasets = DatasetRepository(self.session)
        self.search_logs = SearchLogRepository(self.session)
        self.clicks = ClickRepository(self.session)
        self.enrichment_logs = EnrichmentLogRepository(self.session)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.session is None:
            return

        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        finally:
            await self.session.close()
            self.session = None

    async def commit(self) -> None:
        if self.session is None:
            raise RuntimeError("UnitOfWork session is not initialized")
        await self.session.commit()

    async def rollback(self) -> None:
        if self.session is None:
            raise RuntimeError("UnitOfWork session is not initialized")
        await self.session.rollback()
