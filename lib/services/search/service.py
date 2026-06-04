import logging
import time
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.services.datasets.click_repository import ClickRepository
from lib.services.datasets.models import Dataset
from lib.services.datasets.repository import DatasetRepository
from lib.services.datasets.search_log_repository import SearchLogRepository
from lib.services.datasets.schemas import (
    DatasetItem,
    ScoreBreakdown,
    SearchFilters,
    SearchResponse,
    TopDatasetItem,
    TopSearchResponse,
)
from lib.services.datasets.ml.embedder import EmbeddingService
from lib.services.search.scorers.relevance_ranker import RelevanceRanker

SEARCH_BUFFER_MULTIPLIER = 5


class SearchService:
    def __init__(
        self,
        dataset_repo: DatasetRepository,
        search_log_repo: SearchLogRepository,
        click_repo: ClickRepository,
        embedder: EmbeddingService,
        ranker: RelevanceRanker,
        logger: logging.Logger,
    ):
        self._dataset_repo = dataset_repo
        self._search_log_repo = search_log_repo
        self._click_repo = click_repo
        self._embedder = embedder
        self._ranker = ranker
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

        candidate_limit = limit * SEARCH_BUFFER_MULTIPLIER

        raw_results = await self._dataset_repo.vector_search(
            session,
            query_embedding,
            filters,
            limit=candidate_limit,
        )

        bm25_results = None
        if self._ranker.strategy == "v3_rrf":
            bm25_results = await self._dataset_repo.fts_search(
                session,
                query=query,
                filters=filters,
                limit=candidate_limit,
            )

        ranked = self._ranker.rank(raw_results, bm25_results)
        paginated = ranked[offset: offset + limit]

        items = [
            self._to_dataset_item(dataset, breakdown)
            for dataset, _, breakdown in paginated
        ]
        result_ids = [str(dataset.id) for dataset, _, _ in paginated]

        latency_ms = round((time.perf_counter() - start) * 1000, 2)

        search_log = await self._search_log_repo.log_search(
            session,
            user_id=user_id,
            query=query,
            filters=filters.model_dump(exclude_none=True) or None,
            result_count=len(raw_results),
            latency_ms=latency_ms,
            result_ids=result_ids,
            score_version=self._ranker.strategy,
        )

        self._logger.info(
            f"Search completed: {len(items)} items in {latency_ms}ms "
            f"strategy={self._ranker.strategy}"
        )

        return SearchResponse(
            items=items,
            total=len(ranked),
            execution_time_ms=latency_ms,
            search_log_id=search_log.id,
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

    async def record_click(
        self,
        session: AsyncSession,
        user_id: UUID,
        dataset_id: UUID,
        search_log_id: UUID | None,
        position: int,
    ) -> None:
        await self._click_repo.record_click(
            session,
            user_id=user_id,
            dataset_id=dataset_id,
            search_log_id=search_log_id,
            position=position,
        )
        self._logger.info(
            f"Click recorded: user={user_id} dataset={dataset_id} "
            f"position={position} search_log={search_log_id}"
        )

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
