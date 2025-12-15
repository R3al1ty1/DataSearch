from datetime import datetime
from typing import Optional

from sqlalchemy import String, Text, Boolean, Float, DateTime, Index, ARRAY, JSON
from sqlalchemy.orm import Mapped, mapped_column
from pgvector.sqlalchemy import Vector

from lib.models.base import Base, TimestampMixin, UUIDMixin


class Dataset(Base, UUIDMixin, TimestampMixin):
    """Model for storing dataset metadata from external sources."""
    __tablename__ = "datasets"

    source_name: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        comment="Data source (kaggle, hf, zenodo)"
    )

    external_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Dataset ID on external source"
    )

    title: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Dataset title"
    )

    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Dataset description"
    )

    url: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="Dataset URL"
    )

    column_names: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(Text),
        nullable=True,
        comment="List of column names (for structured data)"
    )

    meta: Mapped[Optional[dict]] = mapped_column(
        JSON,
        nullable=True,
        comment="Additional metadata: license, file_format, size_bytes, tags, etc."
    )

    embedding: Mapped[Optional[list[float]]] = mapped_column(
        Vector(384),
        nullable=True,
        comment="Vector representation for semantic search"
    )

    static_score: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
        comment="Dataset quality score (0.0 - 1.0), computed offline"
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        index=True,
        comment="Active flag (soft delete, for broken links)"
    )

    last_checked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last availability check timestamp"
    )

    source_created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Dataset creation date on source"
    )

    __table_args__ = (
        Index(
            "idx_unique_external_dataset",
            "source_name",
            "external_id",
            unique=True
        ),

        {"comment": "Table for storing dataset metadata from external sources"}
    )

    def __repr__(self) -> str:
        title_preview = self.title[:50] if self.title else ""
        return f"<Dataset(id={self.id}, source={self.source_name}, external_id={self.external_id}, title={title_preview})>"

    @property
    def has_embedding(self) -> bool:
        """Check if embedding exists."""
        return self.embedding is not None

    @property
    def is_ready_for_search(self) -> bool:
        """Check if dataset is ready for search."""
        return self.is_active and self.has_embedding
