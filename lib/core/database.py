import logging
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncEngine
)
from lib.core.constants import AppEnvironment


class DatabaseManager:
    """Database manager for handling connections and sessions."""
    def __init__(
        self,
        dsn: str,
        environment: AppEnvironment,
        logger: logging.Logger
    ):
        self._dsn = dsn
        self._environment = environment
        self._logger = logger

        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker | None = None

    def init(self) -> None:
        """Initializes the database engine and session factory."""
        if self._engine:
            return

        self._engine = create_async_engine(
            self._dsn,
            echo=(self._environment == AppEnvironment.LOCAL),
            future=True,
            pool_pre_ping=True
        )

        self._session_factory = async_sessionmaker(
            bind=self._engine,
            autoflush=False,
            expire_on_commit=False,
            class_=AsyncSession
        )
        self._logger.info("Database initialized successfully.")

    async def close(self) -> None:
        """Closes the database connection."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            self._logger.info("Database connection closed.")

    async def get_session_generator(
        self
    ) -> AsyncGenerator[AsyncSession, None]:
        """Yields an asynchronous database session."""
        if not self._session_factory:
            raise RuntimeError("Database not initialized")

        async with self._session_factory() as session:
            try:
                yield session

            except Exception as e:
                self._logger.error(f"DB Session rollback: {e}")
                await session.rollback()
                raise

            finally:
                await session.close()

    @property
    def engine(self) -> AsyncEngine:
        """Returns the database engine."""
        if not self._engine:
            raise RuntimeError("Database not initialized")

        return self._engine
