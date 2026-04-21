from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from lib.services.static_scores.service import StaticScoreService


def _make_dataset(**kwargs):
    d = MagicMock()
    d.id = kwargs.get("id", uuid4())
    d.source_name = kwargs.get("source_name", "kaggle")
    d.description = kwargs.get("description", "A dataset with enough description to be useful here")
    d.column_names = kwargs.get("column_names", ["age", "income", "label"])
    d.tags = kwargs.get("tags", ["tabular"])
    d.row_count = kwargs.get("row_count", 1000)
    d.total_size_bytes = kwargs.get("total_size_bytes", None)
    d.source_updated_at = kwargs.get("source_updated_at", datetime.now(timezone.utc))
    d.file_formats = kwargs.get("file_formats", ["parquet"])
    d.license = kwargs.get("license", "mit")
    d.download_count = kwargs.get("download_count", 100)
    d.view_count = kwargs.get("view_count", 50)
    d.like_count = kwargs.get("like_count", 10)
    return d


def _make_service(datasets: list):
    repo = MagicMock()
    repo.get_datasets_for_scoring = AsyncMock(return_value=datasets)
    repo.batch_update_static_scores = AsyncMock(return_value=len(datasets))
    repo.commit = AsyncMock()
    return StaticScoreService(dataset_repo=repo, logger=MagicMock())


class TestComputeAll:
    @pytest.mark.anyio
    async def test_empty_corpus_returns_zero(self):
        service = _make_service([])
        result = await service.compute_all(MagicMock())
        assert result == 0

    @pytest.mark.anyio
    async def test_returns_count_of_updated(self):
        datasets = [_make_dataset() for _ in range(5)]
        service = _make_service(datasets)
        result = await service.compute_all(MagicMock())
        assert result == 5

    @pytest.mark.anyio
    async def test_scores_saved_with_all_components(self):
        ds = _make_dataset()
        service = _make_service([ds])
        session = MagicMock()
        await service.compute_all(session)

        call_args = service._repo.batch_update_static_scores.call_args
        scores_dict = call_args[0][1]
        entry = scores_dict[ds.id]

        assert "static_score" in entry
        assert "docs_score" in entry
        assert "repr_score" in entry
        assert "social_score" in entry
        assert "legal_score" in entry

    @pytest.mark.anyio
    async def test_static_score_in_range(self):
        ds = _make_dataset()
        service = _make_service([ds])
        await service.compute_all(MagicMock())

        scores_dict = service._repo.batch_update_static_scores.call_args[0][1]
        score = scores_dict[ds.id]["static_score"]
        assert 0.0 <= score <= 1.0

    @pytest.mark.anyio
    async def test_hf_dataset_no_view_penalty(self):
        hf_ds = _make_dataset(source_name="huggingface", view_count=0)
        kaggle_ds = _make_dataset(source_name="kaggle", view_count=0)
        datasets = [hf_ds, kaggle_ds]
        service = _make_service(datasets)
        await service.compute_all(MagicMock())

        scores = service._repo.batch_update_static_scores.call_args[0][1]
        hf_social = scores[hf_ds.id]["social_score"]
        kaggle_social = scores[kaggle_ds.id]["social_score"]
        assert hf_social >= 0.4
        assert kaggle_social >= 0.4

    @pytest.mark.anyio
    async def test_per_source_percentiles_independent(self):
        hf_popular = _make_dataset(source_name="huggingface", download_count=10000, like_count=500)
        hf_new = _make_dataset(source_name="huggingface", download_count=0, like_count=0)
        kaggle_avg = _make_dataset(source_name="kaggle", download_count=100, view_count=50, like_count=10)
        service = _make_service([hf_popular, hf_new, kaggle_avg])
        await service.compute_all(MagicMock())

        scores = service._repo.batch_update_static_scores.call_args[0][1]
        assert scores[hf_popular.id]["social_score"] > scores[hf_new.id]["social_score"]
        assert scores[kaggle_avg.id]["social_score"] >= 0.4


class TestSocialPercentiles:
    def test_single_dataset_does_not_crash(self):
        service = _make_service([])
        ds = _make_dataset(download_count=100, like_count=10)
        percentiles = service._build_social_percentiles([ds])
        assert "kaggle" in percentiles

    def test_normalize_clips_to_0_1(self):
        assert StaticScoreService._normalize(100.0, (0.0, 1.0)) == 1.0
        assert StaticScoreService._normalize(-5.0, (0.0, 1.0)) == 0.0
        assert StaticScoreService._normalize(0.5, (0.0, 1.0)) == 0.5
