from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Float, Integer, Text, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from lib.models.base import Base, UUIDMixin


class SearchLog(Base, UUIDMixin):
    """Log of user search queries for analytics."""
    __tablename__ = "search_logs"

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=False,
        index=True
    )

    query: Mapped[str] = mapped_column(Text, nullable=False)

    filters: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    result_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0
    )

    latency_ms: Mapped[float] = mapped_column(Float, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True
    )
