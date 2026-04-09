from datetime import datetime

from sqlalchemy import String, Boolean, Index
from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM
from sqlalchemy.orm import Mapped, mapped_column

from lib.models.base import Base, TimestampMixin, UUIDMixin
from lib.core.constants import UserRole


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
