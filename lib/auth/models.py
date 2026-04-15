from datetime import datetime
from sqlalchemy import String, Boolean, Index
from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM
from sqlalchemy.orm import Mapped, mapped_column
from lib.core.base_model import Base, TimestampMixin, UUIDMixin
from lib.core.constants import UserRole
from uuid import UUID
from sqlalchemy import String, Text, Index, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from lib.core.base_model import Base, UUIDMixin

class User(Base, UUIDMixin, TimestampMixin):
    """User model for authentication."""
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True
    )

    password_hash: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True  # NULL for OAuth-only users
    )

    full_name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True
    )

    role: Mapped[str] = mapped_column(
        PG_ENUM(
            UserRole.USER.value,
            UserRole.ADMIN.value,
            name='user_role_enum',
            create_type=False
        ),
        nullable=False,
        default=UserRole.USER.value,
        server_default=UserRole.USER.value,
        index=True
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true"
    )

    is_email_verified: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false"
    )

    last_login_at: Mapped[datetime | None] = mapped_column(
        nullable=True
    )

    # OAuth fields
    oauth_provider: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True
    )

    oauth_provider_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True
    )

    __table_args__ = (
        Index(
            "idx_unique_oauth_user",
            "oauth_provider",
            "oauth_provider_id",
            unique=True,
            postgresql_where="oauth_provider IS NOT NULL"
        ),
    )

    def __repr__(self) -> str:
        return f"<User(id={self.id}, email={self.email}, role={self.role})>"

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
