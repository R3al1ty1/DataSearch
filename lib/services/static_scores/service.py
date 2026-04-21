import logging
import math
from collections import defaultdict
from uuid import UUID

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from lib.services.static_scores.aggregator import CobbDouglasAggregator
from lib.services.static_scores.components import DocsScorer, LegalScorer, ReprScorer, SocialScorer
from lib.services.static_scores.constants import (
    ALL_SIGNALS,
    SOCIAL_PERCENTILE_HIGH,
    SOCIAL_PERCENTILE_LOW,
    SOURCE_AVAILABLE_SIGNALS,
)


class StaticScoreService:
    def __init__(self, dataset_repo, logger: logging.Logger) -> None:
        self._repo = dataset_repo
        self._logger = logger
        self._docs = DocsScorer()
        self._repr = ReprScorer()
        self._legal = LegalScorer()
        self._social = SocialScorer()
        self._aggregator = CobbDouglasAggregator()

    async def compute_all(self, session: AsyncSession) -> int:
        datasets = await self._repo.get_datasets_for_scoring(session)
        if not datasets:
            return 0

        percentiles = self._build_social_percentiles(datasets)

        scores: dict[UUID, dict] = {}
        for ds in datasets:
            docs = self._docs.score(
                description=ds.description,
                column_names=ds.column_names,
                tags=ds.tags,
                row_count=ds.row_count,
                total_size_bytes=ds.total_size_bytes,
                source_updated_at=ds.source_updated_at,
            )
            repr_ = self._repr.score(ds.file_formats)
            legal = self._legal.score(ds.license)
            social = self._social_score_for(ds, percentiles)

            scores[ds.id] = {
                "docs_score": docs,
                "repr_score": repr_,
                "social_score": social,
                "legal_score": legal,
                "static_score": self._aggregator.combine(docs, repr_, social, legal),
            }

        updated = await self._repo.batch_update_static_scores(session, scores)
        await self._repo.commit(session)
        return updated

    def _social_score_for(self, ds, percentiles: dict) -> float:
        available = SOURCE_AVAILABLE_SIGNALS.get(ds.source_name, ALL_SIGNALS)
        src_p = percentiles.get(ds.source_name, {})

        norm_downloads = (
            self._normalize(math.log1p(ds.download_count or 0), src_p.get("downloads"))
            if "downloads" in available else None
        )
        norm_views = (
            self._normalize(math.log1p(ds.view_count or 0), src_p.get("views"))
            if "views" in available else None
        )
        norm_likes = (
            self._normalize(math.log1p(ds.like_count or 0), src_p.get("likes"))
            if "likes" in available else None
        )
        return self._social.score(norm_downloads, norm_views, norm_likes)

    def _build_social_percentiles(self, datasets) -> dict[str, dict[str, tuple[float, float]]]:
        buckets: dict[str, dict[str, list[float]]] = defaultdict(
            lambda: {"downloads": [], "views": [], "likes": []}
        )
        for ds in datasets:
            available = SOURCE_AVAILABLE_SIGNALS.get(ds.source_name, ALL_SIGNALS)
            src = ds.source_name
            if "downloads" in available:
                buckets[src]["downloads"].append(math.log1p(ds.download_count or 0))
            if "views" in available:
                buckets[src]["views"].append(math.log1p(ds.view_count or 0))
            if "likes" in available:
                buckets[src]["likes"].append(math.log1p(ds.like_count or 0))

        result: dict[str, dict[str, tuple[float, float]]] = {}
        for src, signals in buckets.items():
            result[src] = {}
            for signal, values in signals.items():
                if not values:
                    continue
                if len(values) < 2:
                    result[src][signal] = (0.0, values[0] or 1.0)
                else:
                    p_low = float(np.percentile(values, SOCIAL_PERCENTILE_LOW))
                    p_high = float(np.percentile(values, SOCIAL_PERCENTILE_HIGH))
                    result[src][signal] = (p_low, p_high if p_high > p_low else p_low + 1.0)
        return result

    @staticmethod
    def _normalize(value: float, percentile_range: tuple[float, float] | None) -> float:
        if percentile_range is None:
            return 0.0
        p_low, p_high = percentile_range
        return max(0.0, min(1.0, (value - p_low) / (p_high - p_low)))
