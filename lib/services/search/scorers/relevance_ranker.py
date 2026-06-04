from uuid import UUID

from lib.services.datasets.models import Dataset
from lib.services.datasets.schemas import ScoreBreakdown
from lib.services.search.scorers.freshness_scorer import FreshnessScorer

# Linear interpolation strategies: (α_semantic, β_static, γ_freshness)
RANKING_STRATEGIES: dict[str, tuple[float, float, float]] = {
    "v1_hybrid":    (0.70, 0.30, 0.00),
    "v2_freshness": (0.60, 0.25, 0.15),
}

_RRF_STRATEGIES = {"v3_rrf"}
_DEFAULT_STRATEGY = "v1_hybrid"
_RRF_K = 60


class RelevanceRanker:
    """
    Hybrid ranker supporting two modes:
    - Linear interpolation (v1_hybrid, v2_freshness): α·semantic + β·static + γ·freshness
    - RRF + BM25 (v3_rrf): RRF(semantic, bm25) × quality_boost(static, freshness)
    """

    def __init__(self, freshness_scorer: FreshnessScorer, strategy: str = _DEFAULT_STRATEGY):
        self._freshness_scorer = freshness_scorer
        self._strategy = (
            strategy
            if strategy in RANKING_STRATEGIES or strategy in _RRF_STRATEGIES
            else _DEFAULT_STRATEGY
        )

    @property
    def strategy(self) -> str:
        return self._strategy

    def rank(
        self,
        semantic_results: list[tuple[Dataset, float]],
        bm25_results: list[tuple[Dataset, float]] | None = None,
    ) -> list[tuple[Dataset, float, ScoreBreakdown]]:
        """
        Ranks datasets by hybrid score, descending.

        Args:
            semantic_results: (dataset, cosine_distance) from ANN search.
            bm25_results: (dataset, ts_rank) from FTS search. Required for v3_rrf.
        """
        if self._strategy in _RRF_STRATEGIES and bm25_results is not None:
            return self._rank_rrf(semantic_results, bm25_results)
        return self._rank_linear(semantic_results)

    def _rank_linear(
        self,
        results: list[tuple[Dataset, float]],
    ) -> list[tuple[Dataset, float, ScoreBreakdown]]:
        """Linear interpolation: α·semantic + β·static + γ·freshness."""
        α, β, γ = RANKING_STRATEGIES[
            self._strategy if self._strategy in RANKING_STRATEGIES else _DEFAULT_STRATEGY
        ]
        ranked = [
            (dataset, breakdown.final_score, breakdown)
            for dataset, cosine_distance in results
            for breakdown in (self._linear_scores(dataset, cosine_distance, α, β, γ),)
        ]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked

    def _rank_rrf(
        self,
        semantic_results: list[tuple[Dataset, float]],
        bm25_results: list[tuple[Dataset, float]],
    ) -> list[tuple[Dataset, float, ScoreBreakdown]]:
        """
        RRF fusion of semantic + BM25, then quality boost by static + freshness.
        final = rrf_score × (0.5 + 0.3·static + 0.2·freshness)
        """
        rrf: dict[UUID, float] = {}
        sem_map: dict[UUID, float] = {}
        bm25_map: dict[UUID, float] = {}
        datasets: dict[UUID, Dataset] = {}

        for rank, (dataset, distance) in enumerate(semantic_results):
            datasets[dataset.id] = dataset
            rrf[dataset.id] = rrf.get(dataset.id, 0.0) + 1.0 / (_RRF_K + rank + 1)
            sem_map[dataset.id] = round(max(0.0, 1.0 - distance), 4)

        for rank, (dataset, ts_rank) in enumerate(bm25_results):
            datasets[dataset.id] = dataset
            rrf[dataset.id] = rrf.get(dataset.id, 0.0) + 1.0 / (_RRF_K + rank + 1)
            bm25_map[dataset.id] = round(ts_rank, 4)

        results = []
        for dataset_id, rrf_score in rrf.items():
            dataset = datasets[dataset_id]
            static = round(dataset.static_score or 0.0, 4)
            freshness = self._freshness_scorer.score(dataset.source_updated_at)
            quality = 0.5 + 0.3 * static + 0.2 * freshness
            final = round(rrf_score * quality, 6)
            breakdown = ScoreBreakdown(
                semantic_score=sem_map.get(dataset_id, 0.0),
                bm25_score=bm25_map.get(dataset_id, 0.0),
                static_score=static,
                freshness_score=freshness,
                final_score=final,
            )
            results.append((dataset, final, breakdown))

        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def _linear_scores(
        self,
        dataset: Dataset,
        cosine_distance: float,
        α: float,
        β: float,
        γ: float,
    ) -> ScoreBreakdown:
        semantic  = max(0.0, round(1.0 - cosine_distance, 4))
        static    = round(dataset.static_score or 0.0, 4)
        freshness = self._freshness_scorer.score(dataset.source_updated_at)
        final     = round(α * semantic + β * static + γ * freshness, 4)
        return ScoreBreakdown(
            semantic_score=semantic,
            static_score=static,
            freshness_score=freshness,
            final_score=final,
        )
