from datetime import datetime
from uuid import UUID

from sqlalchemy import String, Text, Index, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB

from lib.models.base import Base, UUIDMixin


class SecurityEvent(Base, UUIDMixin):
    """Security event logging for audit trail."""
    __tablename__ = "security_events"

    user_id: Mapped[UUID | None] = mapped_column(
        nullable=True,
        index=True
    )

    event_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True
    )

    ip_address: Mapped[str | None] = mapped_column(
        String(45),
        nullable=True
    )

    user_agent: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    details: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )

    __table_args__ = (
        Index("idx_security_events_created_at", "created_at"),
        Index("idx_security_events_type_created", "event_type", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<SecurityEvent(id={self.id}, type={self.event_type}, user_id={self.user_id})>"
