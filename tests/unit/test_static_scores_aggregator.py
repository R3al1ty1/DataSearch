import pytest

from lib.services.static_scores.aggregator import CobbDouglasAggregator


class TestCobbDouglas:
    def setup_method(self):
        self.agg = CobbDouglasAggregator()

    def test_all_ones_gives_one(self):
        assert self.agg.combine(1.0, 1.0, 1.0, 1.0) == 1.0

    def test_worst_case_above_zero(self):
        score = self.agg.combine(0.15, 0.20, 0.40, 0.30)
        assert score > 0.0
        assert score < 0.30

    def test_worst_case_approx(self):
        score = self.agg.combine(0.15, 0.20, 0.40, 0.30)
        assert abs(score - 0.23) < 0.02

    def test_result_in_range(self):
        score = self.agg.combine(0.7, 0.8, 0.6, 0.9)
        assert 0.0 < score <= 1.0

    def test_weak_legal_penalizes_heavily(self):
        good = self.agg.combine(0.9, 0.9, 0.9, 0.9)
        bad_legal = self.agg.combine(0.9, 0.9, 0.9, 0.3)
        assert bad_legal < good

    def test_weak_docs_penalizes_most(self):
        weak_docs = self.agg.combine(0.3, 0.9, 0.9, 0.9)
        weak_repr = self.agg.combine(0.9, 0.3, 0.9, 0.9)
        assert weak_docs < weak_repr

    def test_penalty_multiplicative_not_additive(self):
        additive_estimate = 0.40 * 0.3 + 0.15 * 0.9 + 0.25 * 0.9 + 0.20 * 0.9
        multiplicative = self.agg.combine(0.3, 0.9, 0.9, 0.9)
        assert multiplicative < additive_estimate

    def test_output_rounded_to_4_decimals(self):
        score = self.agg.combine(0.7, 0.8, 0.6, 0.9)
        parts = str(score).split(".")
        assert len(parts[-1]) <= 4

    @pytest.mark.parametrize("docs,repr_,social,legal", [
        (1.0, 1.0, 1.0, 1.0),
        (0.15, 0.20, 0.40, 0.30),
        (0.5, 0.5, 0.5, 0.5),
        (0.8, 0.9, 0.7, 1.0),
    ])
    def test_monotone_in_range(self, docs, repr_, social, legal):
        score = self.agg.combine(docs, repr_, social, legal)
        assert 0.0 <= score <= 1.0
