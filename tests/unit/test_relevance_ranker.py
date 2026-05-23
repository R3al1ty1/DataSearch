"""Unit tests for RelevanceRanker."""
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4


from lib.services.search.freshness_scorer import FreshnessScorer
from lib.services.search.relevance_ranker import RANKING_STRATEGIES, _RRF_K, RelevanceRanker


def _make_dataset(static_score: float | None = None, source_updated_at=None, **kwargs):
    d = MagicMock()
    d.id = kwargs.get("id", uuid4())
    d.static_score = static_score
    d.source_updated_at = source_updated_at
    return d


def _make_ranker(strategy: str = "v1_hybrid") -> RelevanceRanker:
    return RelevanceRanker(freshness_scorer=FreshnessScorer(halflife_days=365), strategy=strategy)


class TestRelevanceRankerV1Hybrid:
    def test_higher_semantic_ranks_first(self):
        ranker = _make_ranker("v1_hybrid")
        a = _make_dataset(static_score=0.0)
        b = _make_dataset(static_score=0.0)
        ranked = ranker.rank([(a, 0.1), (b, 0.5)])
        assert ranked[0][0] is a

    def test_higher_static_can_overcome_lower_semantic(self):
        ranker = _make_ranker("v1_hybrid")
        a = _make_dataset(static_score=0.0)
        b = _make_dataset(static_score=1.0)
        # a: 0.7*0.9 + 0.3*0.0 = 0.63
        # b: 0.7*0.5 + 0.3*1.0 = 0.65
        ranked = ranker.rank([(a, 0.1), (b, 0.5)])
        assert ranked[0][0] is b

    def test_none_static_treated_as_zero(self):
        ranker = _make_ranker("v1_hybrid")
        d = _make_dataset(static_score=None)
        ranked = ranker.rank([(d, 0.2)])
        assert ranked[0][2].static_score == 0.0

    def test_cosine_distance_gt_one_clamped(self):
        ranker = _make_ranker("v1_hybrid")
        d = _make_dataset(static_score=0.0)
        ranked = ranker.rank([(d, 1.5)])
        assert ranked[0][2].semantic_score == 0.0

    def test_freshness_weight_zero_in_v1(self):
        ranker = _make_ranker("v1_hybrid")
        # freshness = 0.5 (None), but γ=0 → no effect
        d = _make_dataset(static_score=0.5)
        α, β, γ = RANKING_STRATEGIES["v1_hybrid"]
        assert γ == 0.0
        ranked = ranker.rank([(d, 0.2)])
        breakdown = ranked[0][2]
        expected = round(α * breakdown.semantic_score + β * breakdown.static_score, 4)
        assert breakdown.final_score == expected

    def test_empty_results_returns_empty(self):
        ranker = _make_ranker("v1_hybrid")
        assert ranker.rank([]) == []

    def test_sorted_descending(self):
        ranker = _make_ranker("v1_hybrid")
        datasets = [_make_dataset(static_score=0.0) for _ in range(5)]
        distances = [0.5, 0.1, 0.8, 0.3, 0.6]
        ranked = ranker.rank(list(zip(datasets, distances)))
        scores = [r[1] for r in ranked]
        assert scores == sorted(scores, reverse=True)

    def test_scores_rounded_to_4_decimals(self):
        ranker = _make_ranker("v1_hybrid")
        d = _make_dataset(static_score=0.333333)
        ranked = ranker.rank([(d, 0.333333)])
        b = ranked[0][2]
        for val in (b.semantic_score, b.static_score, b.final_score):
            assert val == round(val, 4)


class TestRelevanceRankerV2Freshness:
    def test_fresher_dataset_ranks_higher(self):
        from datetime import timedelta
        ranker = _make_ranker("v2_freshness")
        fresh = _make_dataset(
            static_score=0.7,
            source_updated_at=datetime.now(timezone.utc) - timedelta(days=10),
        )
        stale = _make_dataset(
            static_score=0.7,
            source_updated_at=datetime.now(timezone.utc) - timedelta(days=700),
        )
        # same cosine distance → freshness decides
        ranked = ranker.rank([(fresh, 0.3), (stale, 0.3)])
        assert ranked[0][0] is fresh

    def test_freshness_score_nonzero(self):
        from datetime import timedelta
        ranker = _make_ranker("v2_freshness")
        d = _make_dataset(
            static_score=0.5,
            source_updated_at=datetime.now(timezone.utc) - timedelta(days=100),
        )
        ranked = ranker.rank([(d, 0.2)])
        assert ranked[0][2].freshness_score > 0.0

    def test_freshness_included_in_final_score(self):
        from datetime import timedelta
        ranker = _make_ranker("v2_freshness")
        α, β, γ = RANKING_STRATEGIES["v2_freshness"]
        assert γ > 0.0
        d = _make_dataset(
            static_score=0.5,
            source_updated_at=datetime.now(timezone.utc) - timedelta(days=100),
        )
        ranked = ranker.rank([(d, 0.2)])
        b = ranked[0][2]
        expected = round(α * b.semantic_score + β * b.static_score + γ * b.freshness_score, 4)
        assert b.final_score == expected


class TestRelevanceRankerStrategy:
    def test_unknown_strategy_falls_back_to_v1(self):
        ranker = _make_ranker("nonexistent_strategy")
        assert ranker.strategy == "v1_hybrid"

    def test_strategy_property(self):
        ranker = _make_ranker("v2_freshness")
        assert ranker.strategy == "v2_freshness"

    def test_v3_rrf_strategy_accepted(self):
        ranker = _make_ranker("v3_rrf")
        assert ranker.strategy == "v3_rrf"


class TestRelevanceRankerRRF:
    def _make_rrf_ranker(self) -> RelevanceRanker:
        return RelevanceRanker(freshness_scorer=FreshnessScorer(halflife_days=365), strategy="v3_rrf")

    def test_dataset_in_both_lists_scores_higher(self):
        ranker = self._make_rrf_ranker()
        shared = _make_dataset(static_score=0.5)
        only_sem = _make_dataset(static_score=0.5)

        sem = [(shared, 0.2), (only_sem, 0.3)]
        bm25 = [(shared, 0.8)]

        ranked = ranker.rank(sem, bm25)
        ids = [r[0].id for r in ranked]
        assert ids[0] == shared.id

    def test_rrf_score_uses_rank_not_raw_score(self):
        ranker = self._make_rrf_ranker()
        # rank=0 in BM25 → 1/(60+1) ≈ 0.0164
        d = _make_dataset(static_score=0.5)
        ranked = ranker.rank([], [(d, 999.0)])
        assert ranked[0][2].bm25_score == round(999.0, 4)
        expected_rrf = 1.0 / (_RRF_K + 1)
        quality = 0.5 + 0.3 * 0.5 + 0.2 * ranked[0][2].freshness_score
        assert abs(ranked[0][1] - round(expected_rrf * quality, 6)) < 1e-9

    def test_semantic_only_when_bm25_empty(self):
        ranker = self._make_rrf_ranker()
        d = _make_dataset(static_score=0.5)
        ranked = ranker.rank([(d, 0.2)], [])
        assert len(ranked) == 1
        assert ranked[0][2].bm25_score == 0.0

    def test_bm25_only_results(self):
        ranker = self._make_rrf_ranker()
        d = _make_dataset(static_score=0.5)
        ranked = ranker.rank([], [(d, 0.5)])
        assert len(ranked) == 1
        assert ranked[0][2].semantic_score == 0.0

    def test_quality_boost_applied(self):
        ranker = self._make_rrf_ranker()
        high_quality = _make_dataset(static_score=1.0)
        low_quality = _make_dataset(static_score=0.0)
        # same rank in BM25 → same RRF; quality boost decides
        ranked = ranker.rank([], [(high_quality, 0.5), (low_quality, 0.3)])
        assert ranked[0][0] is high_quality

    def test_empty_both_lists_returns_empty(self):
        ranker = self._make_rrf_ranker()
        assert ranker.rank([], []) == []

    def test_fallback_to_linear_when_bm25_none(self):
        ranker = self._make_rrf_ranker()
        d = _make_dataset(static_score=0.5)
        # v3_rrf but bm25_results=None → falls back to linear (v1_hybrid default)
        ranked = ranker.rank([(d, 0.2)], None)
        assert len(ranked) == 1
        # bm25_score absent → default 0.0
        assert ranked[0][2].bm25_score == 0.0
