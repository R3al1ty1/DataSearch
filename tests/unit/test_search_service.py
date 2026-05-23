"""Unit tests for SearchService business logic."""
import pytest
from pydantic import ValidationError

from lib.services.datasets.schemas import SearchRequest


class TestSearchRequestValidation:
    def test_query_min_length(self):
        with pytest.raises(ValidationError):
            SearchRequest(query="")

    def test_query_max_length(self):
        with pytest.raises(ValidationError):
            SearchRequest(query="x" * 201)

    def test_limit_bounds(self):
        with pytest.raises(ValidationError):
            SearchRequest(query="test", limit=0)
        with pytest.raises(ValidationError):
            SearchRequest(query="test", limit=51)

    def test_to_filters_excludes_none_fields(self):
        req = SearchRequest(query="test", source_name="kaggle")
        filters = req.to_filters()
        assert filters.source_name == "kaggle"
        assert filters.file_formats is None
        assert filters.license is None

    def test_defaults(self):
        req = SearchRequest(query="test")
        assert req.limit == 10
        assert req.offset == 0
