from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.models.security_event import SecurityEvent
from lib.repositories.base import BaseRepository


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
