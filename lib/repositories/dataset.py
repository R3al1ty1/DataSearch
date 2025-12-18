from uuid import UUID

from sqlalchemy import select, update, and_, or_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.inspection import inspect

from lib.models.dataset import (
    Dataset,
    EnrichmentStatus,
    DatasetFieldsExclude
)
from lib.repositories.base import BaseRepository
from lib.schemas.stats import SourceStats


class DatasetRepository(BaseRepository[Dataset]):
    """Repository for dataset operations."""

    def __init__(self, session: AsyncSession):
        super().__init__(Dataset, session)

    async def get_by_external_id(
        self, source_name: str, external_id: str
    ) -> Dataset | None:
        """Gets dataset by source and external ID."""
        result = await self.session.execute(
            select(Dataset).where(
                and_(
                    Dataset.source_name == source_name,
                    Dataset.external_id == external_id
                )
            )
        )
        return result.scalar_one_or_none()

    async def upsert(self, dataset: Dataset) -> Dataset:
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

        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.scalar_one()

    async def bulk_upsert(self, datasets: list[Dataset]) -> int:
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

        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount

    async def get_pending_for_enrichment(
        self,
        source_name: str,
        limit: int = 100,
        max_attempts: int = 3
    ) -> list[Dataset]:
        """Gets datasets pending API enrichment for specific source."""
        result = await self.session.execute(
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
        self, limit: int = 100
    ) -> list[Dataset]:
        """Gets datasets ready for embedding generation."""
        result = await self.session.execute(
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

    async def mark_enriching(self, dataset_id: UUID) -> None:
        """Marks dataset as currently enriching."""
        await self.session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(
                enrichment_status=EnrichmentStatus.ENRICHING.value,
                enrichment_attempts=Dataset.enrichment_attempts + 1
            )
        )
        await self.session.flush()

    async def mark_enriched(
        self, dataset_id: UUID, embedding: list[float] | None = None
    ) -> None:
        """Marks dataset as fully enriched."""
        values = {
            'enrichment_status': EnrichmentStatus.ENRICHED.value,
            'last_enriched_at': func.now()
        }
        if embedding is not None:
            values['embedding'] = embedding

        await self.session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(**values)
        )
        await self.session.flush()

    async def mark_failed(
        self, dataset_id: UUID, error_message: str
    ) -> None:
        """Marks dataset as failed enrichment."""
        await self.session.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(
                enrichment_status=EnrichmentStatus.FAILED.value,
                last_enrichment_error=error_message,
                is_active=False
            )
        )
        await self.session.flush()

    async def count_by_source(self, source_name: str) -> int:
        """Counts datasets by source."""
        result = await self.session.execute(
            select(func.count(Dataset.id)).where(
                Dataset.source_name == source_name
            )
        )
        return result.scalar_one()

    async def count_by_status(
        self, source_name: str, status: EnrichmentStatus
    ) -> int:
        """Counts datasets by source and enrichment status."""
        result = await self.session.execute(
            select(func.count(Dataset.id)).where(
                and_(
                    Dataset.source_name == source_name,
                    Dataset.enrichment_status == status.value
                )
            )
        )
        return result.scalar_one()

    async def get_stats_by_source(self, source_name: str) -> SourceStats:
        """Gets statistics for a specific source."""
        total = await self.count_by_source(source_name)
        minimal = await self.count_by_status(
            source_name, EnrichmentStatus.MINIMAL
        )
        pending = await self.count_by_status(
            source_name, EnrichmentStatus.PENDING
        )
        enriching = await self.count_by_status(
            source_name, EnrichmentStatus.ENRICHING
        )
        enriched = await self.count_by_status(
            source_name, EnrichmentStatus.ENRICHED
        )
        failed = await self.count_by_status(
            source_name, EnrichmentStatus.FAILED
        )
        skipped = await self.count_by_status(
            source_name, EnrichmentStatus.SKIPPED
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
