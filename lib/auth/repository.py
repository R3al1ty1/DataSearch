from uuid import UUID
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from lib.auth.models import User
from lib.core.base_repository import BaseRepository
from lib.auth.models import SecurityEvent

class UserRepository(BaseRepository[User]):
    """Repository for user operations."""

    def __init__(self):
        super().__init__(User)

    async def get_by_email(self, session: AsyncSession, email: str) -> User | None:
        """Get user by email."""
        result = await session.execute(
            select(User).where(User.email == email.lower())
        )
        return result.scalar_one_or_none()

    async def get_by_oauth(
        self,
        session: AsyncSession,
        provider: str,
        provider_id: str
    ) -> User | None:
        """Get user by OAuth provider and ID."""
        result = await session.execute(
            select(User).where(
                User.oauth_provider == provider,
                User.oauth_provider_id == provider_id
            )
        )
        return result.scalar_one_or_none()

    async def email_exists(self, session: AsyncSession, email: str) -> bool:
        """Check if email already exists."""
        result = await session.execute(
            select(func.count(User.id)).where(User.email == email.lower())
        )
        return result.scalar_one() > 0

    async def update_last_login(self, session: AsyncSession, user_id: UUID) -> None:
        """Update user's last login timestamp."""
        await session.execute(
            update(User)
            .where(User.id == user_id)
            .values(last_login_at=func.now())
        )
        await session.flush()

    async def create_user(
        self,
        session: AsyncSession,
        email: str,
        password_hash: str | None,
        full_name: str | None,
        role: str,
        oauth_provider: str | None = None,
        oauth_provider_id: str | None = None
    ) -> User:
        """Create new user."""
        user = User(
            email=email.lower(),
            password_hash=password_hash,
            full_name=full_name,
            role=role,
            oauth_provider=oauth_provider,
            oauth_provider_id=oauth_provider_id,
            is_active=True,
            is_email_verified=True if oauth_provider else False
        )
        return await self.create(session, user)

class SecurityEventRepository(BaseRepository[SecurityEvent]):
    """Repository for security event logging."""

    def __init__(self):
        super().__init__(SecurityEvent)

    async def log_event(
        self,
        session: AsyncSession,
        event_type: str,
        user_id: UUID | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        details: dict | None = None
    ) -> SecurityEvent:
        """Log a security event."""
        event = SecurityEvent(
            user_id=user_id,
            event_type=event_type,
            ip_address=ip_address,
            user_agent=user_agent,
            details=details or {}
        )
        return await self.create(session, event)
