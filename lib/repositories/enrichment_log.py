from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from lib.models.enrichment_log import (
    DatasetEnrichmentLog,
    EnrichmentStage,
    EnrichmentResult
)
from lib.repositories.base import BaseRepository
from lib.schemas.stats import EnrichmentStageStats, ErrorStats


class EnrichmentLogRepository(BaseRepository[DatasetEnrichmentLog]):
    """Repository for enrichment log operations."""

    def __init__(self):
        super().__init__(DatasetEnrichmentLog)

    async def log_enrichment(
        self,
        session: AsyncSession,
        dataset_id: UUID,
        stage: EnrichmentStage,
        result: EnrichmentResult,
        attempt_number: int,
        duration_ms: int | None = None,
        error_message: str | None = None,
        error_type: str | None = None,
        worker_id: str | None = None,
        task_id: str | None = None
    ) -> DatasetEnrichmentLog:
        """Create enrichment log entry."""
        log = DatasetEnrichmentLog(
            dataset_id=dataset_id,
            stage=stage.value,
            result=result.value,
            attempt_number=attempt_number,
            duration_ms=duration_ms,
            error_message=error_message,
            error_type=error_type,
            worker_id=worker_id,
            task_id=task_id
        )
        return await self.create(session, log)

    async def get_logs_by_dataset(
        self, session: AsyncSession, dataset_id: UUID, limit: int = 50
    ) -> list[DatasetEnrichmentLog]:
        """Get enrichment logs for specific dataset."""
        result = await session.execute(
            select(DatasetEnrichmentLog)
            .where(DatasetEnrichmentLog.dataset_id == dataset_id)
            .order_by(DatasetEnrichmentLog.created_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_failed_logs(
        self,
        session: AsyncSession,
        since: datetime | None = None,
        limit: int = 100
    ) -> list[DatasetEnrichmentLog]:
        """Get failed enrichment logs."""
        query = select(DatasetEnrichmentLog).where(
            DatasetEnrichmentLog.result == EnrichmentResult.FAILED.value
        )

        if since:
            query = query.where(DatasetEnrichmentLog.created_at >= since)

        query = query.order_by(
            DatasetEnrichmentLog.created_at.desc()
        ).limit(limit)

        result = await session.execute(query)
        return list(result.scalars().all())

    async def get_stats_by_stage_and_result(
        self, session: AsyncSession, hours: int = 24
    ) -> list[EnrichmentStageStats]:
        """Get enrichment statistics grouped by stage and result."""
        since = datetime.utcnow() - timedelta(hours=hours)

        result = await session.execute(
            select(
                DatasetEnrichmentLog.stage,
                DatasetEnrichmentLog.result,
                func.count().label('count'),
                func.avg(DatasetEnrichmentLog.duration_ms).label(
                    'avg_duration_ms'
                )
            )
            .where(DatasetEnrichmentLog.created_at >= since)
            .group_by(
                DatasetEnrichmentLog.stage,
                DatasetEnrichmentLog.result
            )
        )

        return [
            EnrichmentStageStats(
                stage=row.stage,
                result=row.result,
                count=row.count,
                avg_duration_ms=(
                    float(row.avg_duration_ms) if row.avg_duration_ms else None
                )
            )
            for row in result.all()
        ]

    async def get_top_errors(
        self, session: AsyncSession, hours: int = 168, limit: int = 10
    ) -> list[ErrorStats]:
        """Get top error types in the last N hours."""
        since = datetime.utcnow() - timedelta(hours=hours)

        result = await session.execute(
            select(
                DatasetEnrichmentLog.error_type,
                func.count().label('error_count')
            )
            .where(
                and_(
                    DatasetEnrichmentLog.result == (
                        EnrichmentResult.FAILED.value
                    ),
                    DatasetEnrichmentLog.created_at >= since,
                    DatasetEnrichmentLog.error_type.isnot(None)
                )
            )
            .group_by(DatasetEnrichmentLog.error_type)
            .order_by(func.count().desc())
            .limit(limit)
        )

        return [
            ErrorStats(
                error_type=row.error_type,
                count=row.error_count
            )
            for row in result.all()
        ]

    async def count_attempts_by_dataset(
        self, session: AsyncSession, dataset_id: UUID, stage: EnrichmentStage
    ) -> int:
        """Count enrichment attempts for dataset and stage."""
        result = await session.execute(
            select(func.count(DatasetEnrichmentLog.id)).where(
                and_(
                    DatasetEnrichmentLog.dataset_id == dataset_id,
                    DatasetEnrichmentLog.stage == stage.value
                )
            )
        )
        return result.scalar_one()
