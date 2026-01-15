from enum import Enum
from typing import Optional
from uuid import UUID

from sqlalchemy import String, Text, Integer, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from lib.models.base import Base, TimestampMixin, UUIDMixin


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

    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True
    )

    error_type: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True
    )

    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True
    )

    worker_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True
    )

    task_id: Mapped[Optional[str]] = mapped_column(
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
