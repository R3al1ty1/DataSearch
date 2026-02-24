import logging

from lib.core.redis_auth import RedisAuthManager
from lib.core.constants import AuthConstants
from lib.core.exceptions import RateLimitExceeded


class RateLimitService:
    """Service for rate limiting login attempts."""

    def __init__(
        self, redis_manager: RedisAuthManager, logger: logging.Logger
    ):
        self.redis_manager = redis_manager
        self.logger = logger

    async def check_rate_limit(self, identifier: str) -> None:
        """
        Check if identifier has exceeded rate limit.

        Args:
            identifier: Email or IP address

        Raises:
            RateLimitExceeded: If rate limit is exceeded
        """
        key = f"{AuthConstants.RATE_LIMIT_PREFIX}{identifier}"

        async for redis in self.redis_manager.get_session():
            attempts = await redis.get(key)

            if (attempts and
                int(attempts) >= AuthConstants.RATE_LIMIT_LOGIN_ATTEMPTS):
                ttl = await redis.ttl(key)
                self.logger.warning(f"Rate limit exceeded for {identifier}")
                raise RateLimitExceeded(retry_after=ttl)

    async def increment_attempt(self, identifier: str) -> int:
        """Increment login attempt counter."""
        key = f"{AuthConstants.RATE_LIMIT_PREFIX}{identifier}"

        async for redis in self.redis_manager.get_session():
            count = await redis.incr(key)

            if count == 1:
                await redis.expire(
                    key, AuthConstants.RATE_LIMIT_LOGIN_WINDOW_SECONDS
                )

            return count

        return 0

    async def reset_attempts(self, identifier: str) -> None:
        """Reset login attempt counter (on successful login)."""
        key = f"{AuthConstants.RATE_LIMIT_PREFIX}{identifier}"

        async for redis in self.redis_manager.get_session():
            await redis.delete(key)
