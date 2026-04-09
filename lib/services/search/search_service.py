import logging
import time
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.models.dataset import Dataset
from lib.repositories.dataset import DatasetRepository
from lib.repositories.search_log import SearchLogRepository
from lib.schemas.dataset import (
    DatasetItem,
    ScoreBreakdown,
    SearchFilters,
    SearchResponse,
    TopDatasetItem,
    TopSearchResponse,
)
from lib.services.ml.embedder import EmbeddingService

SEMANTIC_WEIGHT = 0.7
STATIC_WEIGHT = 0.3
SEARCH_BUFFER_MULTIPLIER = 2


class SearchService:
    def __init__(
        self,
        dataset_repo: DatasetRepository,
        search_log_repo: SearchLogRepository,
        embedder: EmbeddingService,
        logger: logging.Logger,
    ):
        self._dataset_repo = dataset_repo
        self._search_log_repo = search_log_repo
        self._embedder = embedder
        self._logger = logger

    async def search(
        self,
        session: AsyncSession,
        query: str,
        filters: SearchFilters,
        limit: int,
        offset: int,
        user_id: UUID,
    ) -> SearchResponse:
        start = time.perf_counter()

        self._logger.info(f"Search query='{query}' limit={limit} offset={offset} user={user_id}")

        query_embedding = self._embedder.encode(query).tolist()

        raw_results = await self._dataset_repo.vector_search(
            session,
            query_embedding,
            filters,
            limit=limit * SEARCH_BUFFER_MULTIPLIER,
        )

        ranked = self._rank(raw_results)
        paginated = ranked[offset: offset + limit]

        items = [
            self._to_dataset_item(dataset, breakdown)
            for dataset, _, breakdown in paginated
        ]

        latency_ms = round((time.perf_counter() - start) * 1000, 2)

        await self._search_log_repo.log_search(
            session,
            user_id=user_id,
            query=query,
            filters=filters.model_dump(exclude_none=True) or None,
            result_count=len(raw_results),
            latency_ms=latency_ms,
        )

        self._logger.info(f"Search completed: {len(items)} items returned in {latency_ms}ms")

        return SearchResponse(
            items=items,
            total=len(ranked),
            execution_time_ms=latency_ms,
        )

    async def get_top_datasets(
        self,
        session: AsyncSession,
        limit: int = 5,
    ) -> TopSearchResponse:
        datasets = await self._dataset_repo.get_top_by_static_score(session, limit)
        return TopSearchResponse(
            items=[self._to_top_dataset_item(d) for d in datasets]
        )

    def _rank(
        self,
        results: list[tuple[Dataset, float]],
    ) -> list[tuple[Dataset, float, ScoreBreakdown]]:
        """Hybrid ranking: final_score = α * semantic_score + β * static_score."""
        ranked = []
        for dataset, cosine_distance in results:
            semantic_score = max(0.0, 1.0 - cosine_distance)
            static_score = dataset.static_score or 0.0
            final_score = SEMANTIC_WEIGHT * semantic_score + STATIC_WEIGHT * static_score
            breakdown = ScoreBreakdown(
                semantic_score=round(semantic_score, 4),
                static_score=round(static_score, 4),
                final_score=round(final_score, 4),
            )
            ranked.append((dataset, final_score, breakdown))
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked

    def _to_dataset_item(self, dataset: Dataset, breakdown: ScoreBreakdown) -> DatasetItem:
        return DatasetItem(
            id=dataset.id,
            source_name=dataset.source_name,
            external_id=dataset.external_id,
            title=dataset.title,
            description=dataset.description,
            url=dataset.url,
            tags=dataset.tags,
            license=dataset.license,
            file_formats=dataset.file_formats,
            row_count=dataset.row_count,
            total_size_bytes=dataset.total_size_bytes,
            download_count=dataset.download_count,
            score=breakdown.final_score,
            score_breakdown=breakdown,
            created_at=dataset.created_at,
        )

    def _to_top_dataset_item(self, dataset: Dataset) -> TopDatasetItem:
        return TopDatasetItem(
            id=dataset.id,
            source_name=dataset.source_name,
            title=dataset.title,
            url=dataset.url,
            description=dataset.description,
            download_count=dataset.download_count,
            like_count=dataset.like_count,
            view_count=dataset.view_count,
            score=dataset.static_score or 0.0,
            created_at=dataset.created_at,
        )
