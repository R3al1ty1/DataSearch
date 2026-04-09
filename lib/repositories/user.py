from uuid import UUID

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from lib.models.user import User
from lib.repositories.base import BaseRepository


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
