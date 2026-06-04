"""Unit tests for FreshnessScorer."""
from datetime import datetime, timedelta, timezone

import pytest

from lib.services.search.scorers.freshness_scorer import FreshnessScorer


@pytest.fixture
def scorer():
    return FreshnessScorer(halflife_days=365)


class TestFreshnessScorer:
    def test_none_returns_neutral(self, scorer):
        assert scorer.score(None) == 0.5

    def test_now_returns_one(self, scorer):
        assert scorer.score(datetime.now(timezone.utc)) == 1.0

    def test_halflife_returns_half(self, scorer):
        date = datetime.now(timezone.utc) - timedelta(days=365)
        result = scorer.score(date)
        assert abs(result - 0.5) < 0.01

    def test_two_halflives_returns_quarter(self, scorer):
        date = datetime.now(timezone.utc) - timedelta(days=730)
        result = scorer.score(date)
        assert abs(result - 0.25) < 0.01

    def test_future_date_clamped_to_one(self, scorer):
        future = datetime.now(timezone.utc) + timedelta(days=30)
        assert scorer.score(future) == 1.0

    def test_older_date_scores_lower(self, scorer):
        recent = datetime.now(timezone.utc) - timedelta(days=30)
        old = datetime.now(timezone.utc) - timedelta(days=500)
        assert scorer.score(recent) > scorer.score(old)

    def test_result_rounded_to_4_decimals(self, scorer):
        date = datetime.now(timezone.utc) - timedelta(days=100)
        result = scorer.score(date)
        assert result == round(result, 4)

    @pytest.mark.parametrize("halflife", [30, 180, 365, 730])
    def test_halflife_respected(self, halflife):
        s = FreshnessScorer(halflife_days=halflife)
        date = datetime.now(timezone.utc) - timedelta(days=halflife)
        assert abs(s.score(date) - 0.5) < 0.01
