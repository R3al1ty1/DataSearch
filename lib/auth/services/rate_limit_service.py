import logging

from lib.core.constants import AuthConstants
from lib.core.exceptions import RateLimitExceeded
from lib.core.redis_auth import RedisAuthManager


class RateLimitService:
    """Service for rate limiting login attempts."""

    def __init__(self, redis_manager: RedisAuthManager, logger: logging.Logger):
        self.redis_manager = redis_manager
        self.logger = logger

    async def check_rate_limit(self, identifier: str) -> None:
        """Raise RateLimitExceeded if identifier exceeded allowed attempts."""
        key = f"{AuthConstants.RATE_LIMIT_PREFIX}{identifier}"

        redis = self.redis_manager.get_client()
        try:
            attempts = await redis.get(key)
            if attempts and int(attempts) >= AuthConstants.RATE_LIMIT_LOGIN_ATTEMPTS:
                ttl = await redis.ttl(key)
                self.logger.warning(f"Rate limit exceeded for {identifier}")
                raise RateLimitExceeded(retry_after=ttl)
        finally:
            await redis.close()

    async def increment_attempt(self, identifier: str) -> None:
        """Increment login attempt counter."""
        key = f"{AuthConstants.RATE_LIMIT_PREFIX}{identifier}"

        redis = self.redis_manager.get_client()
        try:
            count = await redis.incr(key)
            if count == 1:
                await redis.expire(key, AuthConstants.RATE_LIMIT_LOGIN_WINDOW_SECONDS)
        finally:
            await redis.close()

    async def reset_attempts(self, identifier: str) -> None:
        """Reset login attempt counter on successful login."""
        key = f"{AuthConstants.RATE_LIMIT_PREFIX}{identifier}"

        redis = self.redis_manager.get_client()
        try:
            await redis.delete(key)
        finally:
            await redis.close()
