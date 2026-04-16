import logging
from dataclasses import dataclass

from lib.auth.models import User
from lib.auth.utils import (
    create_access_token,
    create_refresh_token,
    get_token_expiry,
    get_token_jti,
)
from lib.core.config import Settings
from lib.core.constants import AuthConstants, UserRole
from lib.core.redis_auth import RedisAuthManager


@dataclass
class TokenPair:
    """Pair of access and refresh tokens."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class TokenService:
    """Service for managing JWT tokens and blacklist."""

    def __init__(
        self,
        redis_manager: RedisAuthManager,
        settings: Settings,
        logger: logging.Logger
    ):
        self.redis_manager = redis_manager
        self.settings = settings
        self.logger = logger

    def generate_tokens(self, user: User) -> TokenPair:
        """Generate access and refresh tokens for user."""
        access_token = create_access_token(
            user_id=user.id,
            email=user.email,
            role=UserRole(user.role),
            settings=self.settings
        )
        refresh_token = create_refresh_token(
            user_id=user.id,
            settings=self.settings
        )
        return TokenPair(access_token=access_token, refresh_token=refresh_token)

    async def blacklist_token(self, token: str) -> None:
        """Add token to blacklist."""
        jti = get_token_jti(token, self.settings)
        expiry = get_token_expiry(token, self.settings)
        key = f"{AuthConstants.TOKEN_BLACKLIST_PREFIX}{jti}"

        redis = self.redis_manager.get_client()
        try:
            await redis.set(key, "revoked", ex=expiry)
        finally:
            await redis.close()

        self.logger.info(f"Token blacklisted: {jti}")

    async def is_token_blacklisted(self, token: str) -> bool:
        """Check if token is blacklisted."""
        jti = get_token_jti(token, self.settings)
        key = f"{AuthConstants.TOKEN_BLACKLIST_PREFIX}{jti}"

        redis = self.redis_manager.get_client()
        try:
            return await redis.exists(key) > 0
        finally:
            await redis.close()
