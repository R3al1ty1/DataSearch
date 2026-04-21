import pytest

from lib.services.static_scores.components import (
    DocsScorer,
    LegalScorer,
    ReprScorer,
    SocialScorer,
)


# ── DocsScorer ────────────────────────────────────────────────────────────────

class TestDocsScore:
    def setup_method(self):
        self.scorer = DocsScorer()

    def test_floor_when_all_empty(self):
        assert self.scorer.score(None, None, None, None, None, None) == 0.15

    def test_max_when_all_filled(self):
        from datetime import datetime, timezone
        score = self.scorer.score(
            description="x" * 200,
            column_names=["age", "diagnosis", "outcome"],
            tags=["medical"],
            row_count=1000,
            total_size_bytes=None,
            source_updated_at=datetime.now(timezone.utc),
        )
        assert score == 1.0

    def test_score_in_range(self):
        score = self.scorer.score("Some description", ["col_0"], ["tag"], 100, None, None)
        assert 0.15 <= score <= 1.0

    @pytest.mark.parametrize("description,expected", [
        (None, 0.0),
        ("", 0.0),
        ("short", 0.3),
        ("x" * 49, 0.3),
        ("x" * 50, 0.7),
        ("x" * 199, 0.7),
        ("x" * 200, 1.0),
    ])
    def test_description_tiers(self, description, expected):
        assert self.scorer._description_grade(description) == expected

    @pytest.mark.parametrize("names,expected", [
        (None, 0.0),
        ([], 0.0),
        (["col_0", "col_1", "col_2"], 0.3),
        (["age", "col_0", "diagnosis"], 0.7),
        (["age", "diagnosis", "outcome"], 1.0),
        (["unnamed: 0", "unnamed: 1"], 0.3),
        (["x", "y"], 0.3),
    ])
    def test_column_names_tiers(self, names, expected):
        assert self.scorer._column_quality(names) == expected

    def test_size_or_rows_signal_either_sufficient(self):
        with_row = self.scorer.score(None, None, None, row_count=100, total_size_bytes=None, source_updated_at=None)
        with_size = self.scorer.score(None, None, None, row_count=None, total_size_bytes=1024, source_updated_at=None)
        neither = self.scorer.score(None, None, None, row_count=None, total_size_bytes=None, source_updated_at=None)
        assert with_row == with_size
        assert with_row > neither


# ── ReprScorer ────────────────────────────────────────────────────────────────

class TestReprScore:
    def setup_method(self):
        self.scorer = ReprScorer()

    @pytest.mark.parametrize("formats,expected", [
        (None, 0.3),
        ([], 0.3),
        (["parquet"], 1.0),
        (["csv"], 0.9),
        (["pdf"], 0.2),
        (["xlsx"], 0.6),
        (["unknown_format"], 0.3),
    ])
    def test_known_formats(self, formats, expected):
        assert self.scorer.score(formats) == expected

    def test_max_taken_over_multiple_formats(self):
        assert self.scorer.score(["pdf", "parquet"]) == 1.0
        assert self.scorer.score(["pdf", "csv"]) == 0.9

    def test_case_insensitive(self):
        assert self.scorer.score(["Parquet"]) == 1.0
        assert self.scorer.score(["CSV"]) == 0.9

    def test_floor_is_0_2(self):
        assert self.scorer.score(["pdf"]) == 0.2
        assert self.scorer.score(["docx"]) == 0.2


# ── LegalScorer ───────────────────────────────────────────────────────────────

class TestLegalScore:
    def setup_method(self):
        self.scorer = LegalScorer()

    @pytest.mark.parametrize("license_val,expected", [
        (None, 0.3),
        ("", 0.3),
        ("mit", 1.0),
        ("apache-2.0", 1.0),
        ("cc0-1.0", 1.0),
        ("cc-by-4.0", 1.0),
        ("mpl-2.0", 0.8),
        ("lgpl-3.0", 0.8),
        ("cc-by-sa-4.0", 0.8),
        ("odbl-1.0", 0.8),
        ("gpl-3.0", 0.6),
        ("agpl-3.0", 0.6),
        ("cc-by-nc-4.0", 0.4),
        ("other", 0.3),
        ("custom", 0.3),
        ("proprietary", 0.3),
    ])
    def test_single_license(self, license_val, expected):
        assert self.scorer.score(license_val) == expected

    def test_alias_apache(self):
        assert self.scorer.score("apache") == 1.0

    def test_alias_cc0(self):
        assert self.scorer.score("cc0") == 1.0

    def test_multiple_licenses_takes_max(self):
        assert self.scorer.score(["gpl-3.0", "mit"]) == 1.0
        assert self.scorer.score(["cc-by-nc-4.0", "apache-2.0"]) == 1.0
        assert self.scorer.score(["other", "gpl-3.0"]) == 0.6

    def test_list_with_empty_strings(self):
        assert self.scorer.score(["", None, "mit"]) == 1.0

    def test_floor_is_0_3(self):
        assert self.scorer.score(None) == 0.3
        assert self.scorer.score("other") == 0.3


# ── SocialScorer ──────────────────────────────────────────────────────────────

class TestSocialScore:
    def setup_method(self):
        self.scorer = SocialScorer()

    def test_floor_when_all_none(self):
        assert self.scorer.score(None, None, None) == 0.4

    def test_max_when_all_normalized_to_one(self):
        assert self.scorer.score(1.0, 1.0, 1.0) == 1.0

    def test_floor_is_0_4(self):
        assert self.scorer.score(0.0, 0.0, 0.0) == 0.4

    def test_available_case_redistribution_no_views(self):
        with_views = self.scorer.score(0.8, 0.5, 0.5)
        without_views = self.scorer.score(0.8, None, 0.5)
        assert without_views != with_views

    def test_without_views_higher_dl_weight(self):
        score_full = self.scorer.score(1.0, 0.0, 0.0)
        score_no_views = self.scorer.score(1.0, None, 0.0)
        assert score_no_views > score_full

    def test_score_in_range(self):
        for dl, vi, li in [(0.5, 0.5, 0.5), (1.0, 0.0, 0.0), (0.0, None, 1.0)]:
            s = self.scorer.score(dl, vi, li)
            assert 0.4 <= s <= 1.0
