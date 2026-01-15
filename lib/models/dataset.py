from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
    String, Text, Boolean, Float, DateTime, Index, ARRAY, BIGINT
)
from sqlalchemy.dialects.postgresql import JSONB, ENUM as PG_ENUM
from sqlalchemy.orm import Mapped, mapped_column
from pgvector.sqlalchemy import Vector

from lib.models.base import Base, TimestampMixin, UUIDMixin


class DatasetFieldsExclude:
    """Fields to exclude during upsert operations."""
    ON_INSERT = {'id', 'created_at', 'updated_at'}
    ON_UPDATE = {'id', 'created_at', 'updated_at', 'enrichment_attempts'}


class EnrichmentStatus(str, Enum):
    """Dataset enrichment status."""
    MINIMAL = "minimal"
    PENDING = "pending"
    ENRICHING = "enriching"
    ENRICHED = "enriched"
    FAILED = "failed"
    SKIPPED = "skipped"


class Dataset(Base, UUIDMixin, TimestampMixin):
    """Universal dataset model for all sources."""
    __tablename__ = "datasets"

    source_name: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True
    )

    external_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False
    )

    title: Mapped[str] = mapped_column(
        Text,
        nullable=False
    )

    url: Mapped[str] = mapped_column(
        String(512),
        nullable=False
    )

    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True
    )

    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), nullable=True
    )

    license: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True
    )

    file_formats: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String),
        nullable=True
    )

    total_size_bytes: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        nullable=True
    )

    column_names: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(Text),
        nullable=True
    )

    row_count: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        nullable=True
    )

    download_count: Mapped[int] = mapped_column(
        BIGINT,
        nullable=False,
        default=0,
        server_default="0"
    )

    view_count: Mapped[int] = mapped_column(
        BIGINT,
        nullable=False,
        default=0,
        server_default="0"
    )

    like_count: Mapped[int] = mapped_column(
        BIGINT,
        nullable=False,
        default=0,
        server_default="0"
    )

    source_created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    source_updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    embedding: Mapped[Optional[list[float]]] = mapped_column(
        Vector(384),
        nullable=True
    )

    static_score: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        index=True
    )

    enrichment_status: Mapped[str] = mapped_column(
        PG_ENUM(
            'minimal', 'pending', 'enriching', 'enriched', 'failed', 'skipped',
            name='enrichment_status_enum',
            create_type=False
        ),
        nullable=False,
        default=EnrichmentStatus.MINIMAL.value,
        server_default="minimal",
        index=True
    )

    enrichment_attempts: Mapped[int] = mapped_column(
        nullable=False,
        default=0,
        server_default="0"
    )

    last_enrichment_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True
    )

    last_enriched_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    last_checked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    source_meta: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True
    )

    __table_args__ = (
        Index(
            "idx_unique_external_dataset",
            "source_name",
            "external_id",
            unique=True
        ),
    )

    def __repr__(self) -> str:
        title_preview = self.title[:50] if self.title else ""
        return (
            f"<Dataset(id={self.id}, source={self.source_name}, "
            f"external_id={self.external_id}, title={title_preview})>"
        )

    @property
    def is_ready_for_search(self) -> bool:
        """Check if dataset is ready for search."""
        return (
            self.is_active
            and self.enrichment_status == EnrichmentStatus.ENRICHED.value
            and self.embedding is not None
        )
