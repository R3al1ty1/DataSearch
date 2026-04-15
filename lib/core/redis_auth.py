import logging
from typing import AsyncGenerator
import redis.asyncio as aioredis

class RedisAuthManager:
    """Redis manager for authentication (database 1)."""

    def __init__(self, redis_url: str, logger: logging.Logger):
        self._redis_url = redis_url
        self._logger = logger
        self._pool: aioredis.ConnectionPool | None = None

    def init(self) -> None:
        """Initialize Redis connection pool."""
        if self._pool:
            return

        self._pool = aioredis.ConnectionPool.from_url(
            self._redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        self._logger.info("Redis auth pool initialized")

    async def close(self) -> None:
        """Close Redis connection pool."""
        if self._pool:
            await self._pool.disconnect()
            self._pool = None
            self._logger.info("Redis auth pool closed")

    def get_client(self) -> aioredis.Redis:
        """Return Redis client from pool."""
        if not self._pool:
            raise RuntimeError("Redis not initialized. Call init() first.")
        return aioredis.Redis(connection_pool=self._pool)

    async def get_session(self) -> AsyncGenerator[aioredis.Redis, None]:
        """Yields Redis connection from pool."""
        redis = self.get_client()
        try:
            yield redis
        finally:
            await redis.close()
