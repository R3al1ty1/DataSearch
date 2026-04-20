from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import and_, bindparam, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.inspection import inspect

from lib.core.base_repository import BaseRepository
from lib.services.datasets.models import (
    Dataset,
    DatasetEnrichmentLog,
    DatasetFieldsExclude,
    EnrichmentResult,
    EnrichmentStage,
    EnrichmentStatus,
)
from lib.services.datasets.schemas import (
    EnrichmentStageStats,
    ErrorStats,
    SearchFilters,
    SourceStats,
)


class DatasetRepository(BaseRepository[Dataset]):
    """Repository for dataset operations."""

    def __init__(self):
        super().__init__(Dataset)

    async def get_by_external_id(
        self, session: AsyncSession, source_name: str, external_id: str
    ) -> Dataset | None:
        """Gets dataset by source and external ID."""
        result = await session.execute(
            select(Dataset).where(
                and_(
                    Dataset.source_name == source_name,
                    Dataset.external_id == external_id
                )
            )
        )
        return result.scalar_one_or_none()

    async def upsert(self, session: AsyncSession, dataset: Dataset) -> Dataset:
        """Inserts or update dataset by (source_name, external_id)."""
        insert_values = self._model_to_dict(
            dataset, DatasetFieldsExclude.ON_INSERT
        )
        update_values = self._get_update_fields_from_model(
            dataset, DatasetFieldsExclude.ON_UPDATE
        )

        stmt = insert(Dataset).values(**insert_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=['source_name', 'external_id'],
            set_=update_values
        ).returning(Dataset)

        result = await session.execute(stmt)
        await session.flush()
        return result.scalar_one()

    async def bulk_upsert(self, session: AsyncSession, datasets: list[Dataset]) -> int:
        """Bulk inserts or updates datasets."""
        if not datasets:
            return 0

        values = [
            self._model_to_dict(d, DatasetFieldsExclude.ON_INSERT)
            for d in datasets
        ]
        stmt = insert(Dataset).values(values)
        update_fields = self._get_update_fields_from_excluded(
            stmt, DatasetFieldsExclude.ON_UPDATE
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=['source_name', 'external_id'],
            set_=update_fields
        )

        result = await session.execute(stmt)
        await session.flush()
        return result.rowcount

    async def get_pending_for_enrichment(
        self,
        session: AsyncSession,
        source_name: str,
        limit: int = 100,
        max_attempts: int = 3
    ) -> list[Dataset]:
        """Gets datasets pending API enrichment for specific source."""
        result = await session.execute(
            select(Dataset)
            .where(
                and_(
                    Dataset.source_name == source_name,
                    or_(
                        Dataset.enrichment_status == (
                            EnrichmentStatus.MINIMAL.value
                        ),
                        Dataset.enrichment_status == (
                            EnrichmentStatus.PENDING.value
                        )
                    ),
                    Dataset.enrichment_attempts < max_attempts,
                    Dataset.is_active
                )
            )
            .order_by(Dataset.created_at.asc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_for_embedding_generation(
        self, session: AsyncSession, limit: int = 100
    ) -> list[Dataset]:
        """Gets datasets ready for embedding generation."""
        result = await session.execute(
            select(Dataset)
            .where(
                and_(
                    Dataset.enrichment_status == (
                        EnrichmentStatus.ENRICHED.value
                    ),
                    Dataset.embedding.is_(None),
                    Dataset.is_active
                )
            )
            .limit(limit)
        )
        return list(result.scalars().all())

    async def mark_enriching(self, session: AsyncSession, dataset_id: UUID) -> None:
        """Marks dataset as currently enriching."""
        await session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(
                enrichment_status=EnrichmentStatus.ENRICHING.value,
                enrichment_attempts=Dataset.enrichment_attempts + 1
            )
        )
        await session.flush()

    async def mark_enriched(
        self, session: AsyncSession, dataset_id: UUID, embedding: list[float] | None = None
    ) -> None:
        """Marks dataset as fully enriched."""
        values = {
            'enrichment_status': EnrichmentStatus.ENRICHED.value,
            'last_enriched_at': func.now()
        }
        if embedding is not None:
            values['embedding'] = embedding

        await session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(**values)
        )
        await session.flush()

    async def mark_failed(
        self, session: AsyncSession, dataset_id: UUID, error_message: str
    ) -> None:
        """Marks dataset as failed enrichment."""
        await session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(
                enrichment_status=EnrichmentStatus.FAILED.value,
                last_enrichment_error=error_message,
                is_active=False
            )
        )
        await session.flush()

    async def count_by_source(self, session: AsyncSession, source_name: str) -> int:
        """Counts datasets by source."""
        result = await session.execute(
            select(func.count(Dataset.id)).where(
                Dataset.source_name == source_name
            )
        )
        return result.scalar_one()

    async def count_by_status(
        self, session: AsyncSession, source_name: str, status: EnrichmentStatus
    ) -> int:
        """Counts datasets by source and enrichment status."""
        result = await session.execute(
            select(func.count(Dataset.id)).where(
                and_(
                    Dataset.source_name == source_name,
                    Dataset.enrichment_status == status.value
                )
            )
        )
        return result.scalar_one()

    async def get_stats_by_source(self, session: AsyncSession, source_name: str) -> SourceStats:
        """Gets statistics for a specific source."""
        total = await self.count_by_source(session, source_name)
        minimal = await self.count_by_status(
            session, source_name, EnrichmentStatus.MINIMAL
        )
        pending = await self.count_by_status(
            session, source_name, EnrichmentStatus.PENDING
        )
        enriching = await self.count_by_status(
            session, source_name, EnrichmentStatus.ENRICHING
        )
        enriched = await self.count_by_status(
            session, source_name, EnrichmentStatus.ENRICHED
        )
        failed = await self.count_by_status(
            session, source_name, EnrichmentStatus.FAILED
        )
        skipped = await self.count_by_status(
            session, source_name, EnrichmentStatus.SKIPPED
        )

        return SourceStats(
            source=source_name,
            total=total,
            minimal=minimal,
            pending=pending,
            enriching=enriching,
            enriched=enriched,
            failed=failed,
            skipped=skipped
        )

    def _model_to_dict(
        self, dataset: Dataset, exclude_fields: set[str]
    ) -> dict:
        """Converts dataset model to dict, excluding specified fields."""
        mapper = inspect(Dataset)
        return {
            col.key: getattr(dataset, col.key)
            for col in mapper.columns
            if col.key not in exclude_fields
        }

    def _get_update_fields_from_model(
        self, dataset: Dataset, exclude_fields: set[str]
    ) -> dict:
        """Gets fields for update from model instance."""
        fields = self._model_to_dict(dataset, exclude_fields)
        fields['updated_at'] = func.now()
        return fields

    def _get_update_fields_from_excluded(
        self, stmt, exclude_fields: set[str]
    ) -> dict:
        """Gets fields for bulk update using stmt.excluded."""
        mapper = inspect(Dataset)
        fields = {
            col.key: getattr(stmt.excluded, col.key)
            for col in mapper.columns
            if col.key not in exclude_fields
        }
        fields['updated_at'] = func.now()
        return fields

    async def vector_search(
        self,
        session: AsyncSession,
        query_embedding: list[float],
        filters: SearchFilters,
        limit: int,
    ) -> list[tuple[Dataset, float]]:
        """Performs cosine similarity ANN search via pgvector."""
        distance_col = Dataset.embedding.cosine_distance(query_embedding).label('distance')

        conditions = [
            Dataset.is_active.is_(True),
            Dataset.embedding.is_not(None),
        ]

        if filters.source_name:
            conditions.append(Dataset.source_name == filters.source_name)
        if filters.license:
            conditions.append(Dataset.license == filters.license)
        if filters.file_formats:
            conditions.append(Dataset.file_formats.overlap(filters.file_formats))
        if filters.min_row_count is not None:
            conditions.append(Dataset.row_count >= filters.min_row_count)
        if filters.max_size_bytes is not None:
            conditions.append(Dataset.total_size_bytes <= filters.max_size_bytes)

        stmt = (
            select(Dataset, distance_col)
            .where(and_(*conditions))
            .order_by(distance_col.asc())
            .limit(limit)
        )

        result = await session.execute(stmt)
        return [(row[0], float(row[1])) for row in result.all()]

    async def get_top_by_static_score(
        self,
        session: AsyncSession,
        limit: int = 5,
    ) -> list[Dataset]:
        """Returns datasets with highest static_score."""
        result = await session.execute(
            select(Dataset)
            .where(
                and_(
                    Dataset.is_active.is_(True),
                    Dataset.embedding.is_not(None),
                    Dataset.static_score.is_not(None),
                )
            )
            .order_by(Dataset.static_score.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_stale_for_validation(
        self,
        session: AsyncSession,
        batch_size: int = 200,
        stale_after_hours: int = 48,
    ) -> list[Dataset]:
        """Returns active datasets whose last_checked_at is stale or NULL."""
        threshold = datetime.now(timezone.utc) - timedelta(hours=stale_after_hours)
        result = await session.execute(
            select(Dataset)
            .where(
                and_(
                    Dataset.is_active.is_(True),
                    or_(
                        Dataset.last_checked_at.is_(None),
                        Dataset.last_checked_at < threshold,
                    ),
                )
            )
            .order_by(Dataset.last_checked_at.asc().nulls_first())
            .limit(batch_size)
        )
        return list(result.scalars().all())

    async def bulk_update_check_results(
        self,
        session: AsyncSession,
        results: list,
    ) -> tuple[int, int]:
        """Batch-updates is_active and last_checked_at for checked datasets.

        Returns (total_updated, deactivated_count).
        """
        now = datetime.now(timezone.utc)
        all_ids = [r.dataset_id for r in results]
        deactivated_ids = [r.dataset_id for r in results if not r.is_reachable]

        await session.execute(
            update(Dataset)
            .where(Dataset.id.in_(all_ids))
            .values(last_checked_at=now)
        )

        if deactivated_ids:
            await session.execute(
                update(Dataset)
                .where(Dataset.id.in_(deactivated_ids))
                .values(is_active=False)
            )

        await session.flush()
        return len(all_ids), len(deactivated_ids)


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

    async def get_metric_data_for_scoring(
        self,
        session: AsyncSession,
    ) -> list[tuple[UUID, int, int, int]]:
        """Returns (id, download_count, view_count, like_count) for active enriched datasets."""
        result = await session.execute(
            select(Dataset.id, Dataset.download_count, Dataset.view_count, Dataset.like_count)
            .where(
                and_(
                    Dataset.is_active.is_(True),
                    Dataset.enrichment_status == EnrichmentStatus.ENRICHED.value,
                )
            )
        )
        return list(result.all())

    async def batch_update_static_scores(
        self,
        session: AsyncSession,
        scores: dict[UUID, float],
    ) -> int:
        """Batch updates static_score for a set of datasets."""
        if not scores:
            return 0

        await session.execute(
            update(Dataset)
            .where(Dataset.id == bindparam('bid'))
            .values(static_score=bindparam('score')),
            [{'bid': dataset_id, 'score': score} for dataset_id, score in scores.items()],
        )
        await session.flush()
        return len(scores)

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
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

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
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

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
