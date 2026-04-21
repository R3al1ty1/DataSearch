from datetime import datetime
from enum import Enum
from uuid import UUID

from sqlalchemy import ARRAY, BIGINT, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, Index
from sqlalchemy.dialects.postgresql import JSONB, ENUM as PG_ENUM
from sqlalchemy.orm import Mapped, mapped_column
from pgvector.sqlalchemy import Vector

from lib.core.base_model import Base, TimestampMixin, UUIDMixin

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

    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    tags: Mapped[list[str] | None] = mapped_column(
        ARRAY(String), nullable=True
    )

    license: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True
    )

    file_formats: Mapped[list[str] | None] = mapped_column(
        ARRAY(String),
        nullable=True
    )

    total_size_bytes: Mapped[int | None] = mapped_column(
        BIGINT,
        nullable=True
    )

    column_names: Mapped[list[str] | None] = mapped_column(
        ARRAY(Text),
        nullable=True
    )

    row_count: Mapped[int | None] = mapped_column(
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

    source_created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    source_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    embedding: Mapped[list[float] | None] = mapped_column(
        Vector(384),
        nullable=True
    )

    static_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    docs_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    repr_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    social_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    legal_score: Mapped[float | None] = mapped_column(Float, nullable=True)

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

    last_enrichment_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    last_enriched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    source_meta: Mapped[dict | None] = mapped_column(
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

class EnrichmentStage(str, Enum):
    """Stage of enrichment pipeline."""
    API_METADATA = "api_metadata"
    EMBEDDING = "embedding"
    STATIC_SCORE = "static_score"
    LINK_VALIDATION = "link_validation"

class EnrichmentResult(str, Enum):
    """Result of enrichment attempt."""
    SUCCESS = "success"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    SKIPPED = "skipped"

class DatasetEnrichmentLog(Base, UUIDMixin, TimestampMixin):
    """History of dataset enrichment attempts."""
    __tablename__ = "dataset_enrichment_logs"

    dataset_id: Mapped[UUID] = mapped_column(
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    stage: Mapped[str] = mapped_column(
        String(50),
        nullable=False
    )

    result: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True
    )

    attempt_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )

    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    error_type: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True
    )

    duration_ms: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True
    )

    worker_id: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True
    )

    task_id: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True
    )

    __table_args__ = (
        Index(
            "idx_enrichment_logs_dataset_stage",
            "dataset_id",
            "stage"
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<EnrichmentLog(dataset_id={self.dataset_id}, "
            f"stage={self.stage}, result={self.result})>"
        )


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
    result_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default="now()",
        index=True
    )
