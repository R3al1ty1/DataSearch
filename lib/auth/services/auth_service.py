import logging
from dataclasses import dataclass
from uuid import UUID

from lib.auth.models import User
from lib.auth.services.rate_limit_service import RateLimitService
from lib.auth.services.token_service import TokenPair, TokenService
from lib.auth.utils import (
    decode_token,
    hash_password,
    validate_password,
    verify_password,
)
from lib.auth.exceptions import (
    InvalidCredentials,
    TokenBlacklisted,
    TokenExpired,
    TokenInvalid,
    UserAlreadyExists,
)
from lib.core.config import Settings
from lib.core.constants import UserRole
from lib.core.uow import UnitOfWork


@dataclass
class AuthResult:
    """Result of authentication operation."""
    user: User
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class AuthService:
    """Service for authentication operations."""

    def __init__(
        self,
        token_service: TokenService,
        rate_limit_service: "RateLimitService",
        settings: Settings,
        logger: logging.Logger
    ):
        self.token_service = token_service
        self.rate_limit_service = rate_limit_service
        self.settings = settings
        self.logger = logger

    async def register(
        self,
        uow: UnitOfWork,
        email: str,
        password: str,
        full_name: str | None,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> AuthResult:
        """Register new user."""
        email = email.lower().strip()

        if await uow.users.email_exists(email):
            raise UserAlreadyExists(email)

        validate_password(password)

        password_hash = hash_password(password)

        user = await uow.users.create_user(
            email=email,
            password_hash=password_hash,
            full_name=full_name,
            role=UserRole.USER.value
        )

        await uow.security_events.log_event(
            event_type="user_registered",
            user_id=user.id,
            ip_address=ip_address,
            user_agent=user_agent
        )

        self.logger.info(f"User registered: {email}")

        tokens = self.token_service.generate_tokens(user)

        return AuthResult(
            user=user,
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type
        )

    async def login(
        self,
        uow: UnitOfWork,
        email: str,
        password: str,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> AuthResult:
        """Authenticate user and return tokens."""
        email = email.lower().strip()

        await self.rate_limit_service.check_rate_limit(email)

        user = await uow.users.get_by_email(email)

        if not user or not user.password_hash:
            await self.rate_limit_service.increment_attempt(email)
            await uow.security_events.log_event(
                event_type="login_failed",
                ip_address=ip_address,
                user_agent=user_agent,
                details={"reason": "user_not_found", "email": email}
            )
            await uow.commit()
            raise InvalidCredentials()

        if not verify_password(password, user.password_hash):
            await self.rate_limit_service.increment_attempt(email)
            await uow.security_events.log_event(
                event_type="login_failed",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"reason": "invalid_password"}
            )
            await uow.commit()
            raise InvalidCredentials()

        if not user.is_active:
            await uow.security_events.log_event(
                event_type="login_failed",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"reason": "account_inactive"}
            )
            await uow.commit()
            raise InvalidCredentials()

        await self.rate_limit_service.reset_attempts(email)

        user.last_login_at = await uow.users.update_last_login(user.id)

        await uow.security_events.log_event(
            event_type="login_success",
            user_id=user.id,
            ip_address=ip_address,
            user_agent=user_agent
        )
        self.logger.info(f"User logged in: {email}")

        tokens = self.token_service.generate_tokens(user)

        return AuthResult(
            user=user,
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type
        )

    async def refresh_token(
        self,
        uow: UnitOfWork,
        refresh_token: str
    ) -> TokenPair:
        """Generate new token pair from refresh token (rotation)."""
        if await self.token_service.is_token_blacklisted(refresh_token):
            raise TokenBlacklisted()

        payload = decode_token(refresh_token, self.settings)

        if payload.get("type") != "refresh":
            raise TokenInvalid()

        user_id = UUID(payload["sub"])
        user = await uow.users.get_by_id(user_id)

        if not user or not user.is_active:
            raise TokenInvalid()

        await self.token_service.blacklist_token(refresh_token)

        tokens = self.token_service.generate_tokens(user)

        return tokens

    async def logout(
        self,
        uow: UnitOfWork,
        access_token: str,
        refresh_token: str | None,
        user_id: UUID,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> None:
        """Logout user by blacklisting both access and refresh tokens."""
        await self.token_service.blacklist_token(access_token)

        if refresh_token:
            try:
                await self.token_service.blacklist_token(refresh_token)
            except (TokenExpired, TokenInvalid, KeyError) as exc:
                self.logger.warning(f"Failed to parse refresh token during logout: {exc}")

        await uow.security_events.log_event(
            event_type="logout",
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent
        )

        self.logger.info(f"User logged out: {user_id}")

    async def get_current_user(
        self,
        uow: UnitOfWork,
        token: str
    ) -> User:
        """Get user from access token."""
        if await self.token_service.is_token_blacklisted(token):
            raise TokenBlacklisted()

        payload = decode_token(token, self.settings)

        if payload.get("type") != "access":
            raise TokenInvalid()

        user_id = UUID(payload["sub"])
        user = await uow.users.get_by_id(user_id)

        if not user or not user.is_active:
            raise TokenInvalid()

        return user
