import logging
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.auth.password import hash_password, verify_password, validate_password
from lib.core.auth.jwt import decode_token
from lib.core.config import Settings
from lib.core.constants import UserRole
from lib.core.exceptions import (
    InvalidCredentials,
    UserNotFound,
    UserAlreadyExists,
    TokenInvalid,
    TokenBlacklisted
)
from lib.models.user import User
from lib.repositories.user import UserRepository
from lib.repositories.security_event import SecurityEventRepository
from lib.services.auth.token_service import TokenService, TokenPair


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
        user_repo: UserRepository,
        security_event_repo: SecurityEventRepository,
        token_service: TokenService,
        rate_limit_service: "RateLimitService",
        settings: Settings,
        logger: logging.Logger
    ):
        self.user_repo = user_repo
        self.security_event_repo = security_event_repo
        self.token_service = token_service
        self.rate_limit_service = rate_limit_service
        self.settings = settings
        self.logger = logger

    async def register(
        self,
        session: AsyncSession,
        email: str,
        password: str,
        full_name: str | None,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> AuthResult:
        """Register new user."""
        email = email.lower().strip()

        if await self.user_repo.email_exists(session, email):
            raise UserAlreadyExists(email)

        validate_password(password)

        password_hash = hash_password(password)

        user = await self.user_repo.create_user(
            session=session,
            email=email,
            password_hash=password_hash,
            full_name=full_name,
            role=UserRole.USER.value
        )

        await session.commit()

        await self.security_event_repo.log_event(
            session=session,
            event_type="user_registered",
            user_id=user.id,
            ip_address=ip_address,
            user_agent=user_agent
        )
        await session.commit()

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
        session: AsyncSession,
        email: str,
        password: str,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> AuthResult:
        """Authenticate user and return tokens."""
        email = email.lower().strip()

        await self.rate_limit_service.check_rate_limit(email)

        user = await self.user_repo.get_by_email(session, email)

        if not user or not user.password_hash:
            await self.rate_limit_service.increment_attempt(email)
            await self.security_event_repo.log_event(
                session=session,
                event_type="login_failed",
                ip_address=ip_address,
                user_agent=user_agent,
                details={"reason": "user_not_found", "email": email}
            )
            await session.commit()
            raise InvalidCredentials()

        if not verify_password(password, user.password_hash):
            await self.rate_limit_service.increment_attempt(email)
            await self.security_event_repo.log_event(
                session=session,
                event_type="login_failed",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"reason": "invalid_password"}
            )
            await session.commit()
            raise InvalidCredentials()

        if not user.is_active:
            await self.security_event_repo.log_event(
                session=session,
                event_type="login_failed",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"reason": "account_inactive"}
            )
            await session.commit()
            raise InvalidCredentials()

        await self.rate_limit_service.reset_attempts(email)

        await self.user_repo.update_last_login(session, user.id)

        await self.security_event_repo.log_event(
            session=session,
            event_type="login_success",
            user_id=user.id,
            ip_address=ip_address,
            user_agent=user_agent
        )
        await session.commit()

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
        session: AsyncSession,
        refresh_token: str
    ) -> TokenPair:
        """Generate new access token from refresh token."""
        if await self.token_service.is_token_blacklisted(refresh_token):
            raise TokenBlacklisted()

        payload = decode_token(refresh_token, self.settings)

        if payload.get("type") != "refresh":
            raise TokenInvalid()

        user_id = UUID(payload["sub"])
        user = await self.user_repo.get_by_id(session, user_id)

        if not user or not user.is_active:
            raise UserNotFound(str(user_id))

        tokens = self.token_service.generate_tokens(user)

        return tokens

    async def logout(
        self,
        session: AsyncSession,
        access_token: str,
        user_id: UUID,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> None:
        """Logout user by blacklisting token."""
        await self.token_service.blacklist_token(access_token)

        await self.security_event_repo.log_event(
            session=session,
            event_type="logout",
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent
        )
        await session.commit()

        self.logger.info(f"User logged out: {user_id}")

    async def get_current_user(
        self,
        session: AsyncSession,
        token: str
    ) -> User:
        """Get user from access token."""
        if await self.token_service.is_token_blacklisted(token):
            raise TokenBlacklisted()

        payload = decode_token(token, self.settings)

        if payload.get("type") != "access":
            raise TokenInvalid()

        user_id = UUID(payload["sub"])
        user = await self.user_repo.get_by_id(session, user_id)

        if not user or not user.is_active:
            raise UserNotFound(str(user_id))

        return user
