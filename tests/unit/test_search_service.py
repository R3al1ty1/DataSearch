"""Unit tests for SearchService business logic."""
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from lib.crons.enrichment.static_scores import _compute_scores
from lib.services.search.search_service import (
    SEMANTIC_WEIGHT,
    STATIC_WEIGHT,
    SearchService,
)


def _make_dataset(static_score: float | None = None, **kwargs):
    """Creates a minimal Dataset-like mock."""
    d = MagicMock()
    d.id = kwargs.get('id', uuid4())
    d.source_name = kwargs.get('source_name', 'kaggle')
    d.external_id = kwargs.get('external_id', 'user/dataset')
    d.title = kwargs.get('title', 'Test Dataset')
    d.description = kwargs.get('description', None)
    d.url = kwargs.get('url', 'https://kaggle.com/datasets/user/dataset')
    d.tags = kwargs.get('tags', None)
    d.license = kwargs.get('license', None)
    d.file_formats = kwargs.get('file_formats', None)
    d.row_count = kwargs.get('row_count', None)
    d.total_size_bytes = kwargs.get('total_size_bytes', None)
    d.download_count = kwargs.get('download_count', 0)
    d.like_count = kwargs.get('like_count', 0)
    d.view_count = kwargs.get('view_count', 0)
    d.static_score = static_score
    d.created_at = kwargs.get('created_at', datetime.now(timezone.utc))
    return d


def _make_service() -> SearchService:
    return SearchService(
        dataset_repo=MagicMock(),
        search_log_repo=MagicMock(),
        embedder=MagicMock(),
        logger=MagicMock(),
    )


class TestRank:
    def test_higher_semantic_score_ranks_first(self):
        service = _make_service()
        dataset_a = _make_dataset(static_score=0.0)
        dataset_b = _make_dataset(static_score=0.0)

        # distance 0.1 → semantic 0.9, distance 0.5 → semantic 0.5
        results = [(dataset_a, 0.1), (dataset_b, 0.5)]
        ranked = service._rank(results)

        assert ranked[0][0] is dataset_a
        assert ranked[1][0] is dataset_b

    def test_higher_static_score_can_overcome_lower_semantic(self):
        service = _make_service()
        dataset_a = _make_dataset(static_score=0.0)   # good semantics, no popularity
        dataset_b = _make_dataset(static_score=1.0)   # weaker semantics, very popular

        # dataset_a: semantic=0.9, static=0.0 → final = 0.7*0.9 + 0.3*0.0 = 0.63
        # dataset_b: semantic=0.5, static=1.0 → final = 0.7*0.5 + 0.3*1.0 = 0.65
        results = [(dataset_a, 0.1), (dataset_b, 0.5)]
        ranked = service._rank(results)

        assert ranked[0][0] is dataset_b

    def test_none_static_score_treated_as_zero(self):
        service = _make_service()
        dataset = _make_dataset(static_score=None)
        ranked = service._rank([(dataset, 0.2)])
        breakdown = ranked[0][2]
        assert breakdown.static_score == 0.0

    def test_cosine_distance_greater_than_one_clamped(self):
        service = _make_service()
        dataset = _make_dataset(static_score=0.0)
        ranked = service._rank([(dataset, 1.5)])
        breakdown = ranked[0][2]
        assert breakdown.semantic_score == 0.0

    def test_final_score_is_weighted_combination(self):
        service = _make_service()
        dataset = _make_dataset(static_score=0.8)
        # distance=0.2 → semantic=0.8
        ranked = service._rank([(dataset, 0.2)])
        breakdown = ranked[0][2]
        expected = round(SEMANTIC_WEIGHT * 0.8 + STATIC_WEIGHT * 0.8, 4)
        assert breakdown.final_score == expected

    def test_empty_results_returns_empty(self):
        service = _make_service()
        assert service._rank([]) == []

    def test_score_breakdown_values_rounded_to_4_decimals(self):
        service = _make_service()
        dataset = _make_dataset(static_score=0.333333)
        ranked = service._rank([(dataset, 0.333333)])
        breakdown = ranked[0][2]
        assert len(str(breakdown.semantic_score).split('.')[-1]) <= 4
        assert len(str(breakdown.static_score).split('.')[-1]) <= 4

    def test_sorted_descending_by_final_score(self):
        service = _make_service()
        datasets = [_make_dataset(static_score=0.0) for _ in range(5)]
        distances = [0.5, 0.1, 0.8, 0.3, 0.6]
        results = list(zip(datasets, distances))
        ranked = service._rank(results)

        scores = [r[1] for r in ranked]
        assert scores == sorted(scores, reverse=True)


class TestComputeScores:
    def test_empty_input_returns_empty(self):
        assert _compute_scores([]) == {}

    def test_single_row_gets_max_score(self):
        dataset_id = uuid4()
        scores = _compute_scores([(dataset_id, 100, 50, 20)])
        assert scores[dataset_id] == 1.0

    def test_scores_in_range_zero_to_one(self):
        rows = [(uuid4(), 1000, 500, 100), (uuid4(), 10, 5, 1), (uuid4(), 0, 0, 0)]
        scores = _compute_scores(rows)
        for score in scores.values():
            assert 0.0 <= score <= 1.0

    def test_dataset_with_all_zeros_gets_zero_score(self):
        id_high = uuid4()
        id_zero = uuid4()
        scores = _compute_scores([(id_high, 1000, 500, 100), (id_zero, 0, 0, 0)])
        assert scores[id_zero] == 0.0

    def test_highest_metrics_gets_highest_score(self):
        id_a = uuid4()
        id_b = uuid4()
        scores = _compute_scores([(id_a, 1000, 1000, 1000), (id_b, 1, 1, 1)])
        assert scores[id_a] > scores[id_b]


class TestSearchRequestValidation:
    def test_query_min_length(self):
        from pydantic import ValidationError

        from lib.schemas.dataset import SearchRequest
        with pytest.raises(ValidationError):
            SearchRequest(query="")

    def test_query_max_length(self):
        from pydantic import ValidationError

        from lib.schemas.dataset import SearchRequest
        with pytest.raises(ValidationError):
            SearchRequest(query="x" * 201)

    def test_limit_bounds(self):
        from pydantic import ValidationError

        from lib.schemas.dataset import SearchRequest
        with pytest.raises(ValidationError):
            SearchRequest(query="test", limit=0)
        with pytest.raises(ValidationError):
            SearchRequest(query="test", limit=51)

    def test_to_filters_excludes_none_fields(self):
        from lib.schemas.dataset import SearchRequest
        req = SearchRequest(query="test", source_name="kaggle")
        filters = req.to_filters()
        assert filters.source_name == "kaggle"
        assert filters.file_formats is None
        assert filters.license is None

    def test_defaults(self):
        from lib.schemas.dataset import SearchRequest
        req = SearchRequest(query="test")
        assert req.limit == 10
        assert req.offset == 0
