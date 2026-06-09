from datetime import datetime, timezone

from lib.services.datasets.enrichment.zenodo_parser.client import ZenodoClient


def test_zenodo_client_limits_anonymous_page_size():
    client = object.__new__(ZenodoClient)
    client.headers = {"User-Agent": "test"}

    assert client._page_size(100) == 25


def test_zenodo_client_allows_authenticated_page_size():
    client = object.__new__(ZenodoClient)
    client.headers = {"User-Agent": "test", "Authorization": "Bearer token"}

    assert client._page_size(100) == 100


def test_zenodo_client_extracts_hits_payload():
    client = object.__new__(ZenodoClient)
    payload = {"hits": {"hits": [{"id": 1}, {"id": 2}, "bad"]}}

    assert client._extract_records(payload) == [{"id": 1}, {"id": 2}]


def test_zenodo_client_stops_when_record_is_older_than_cursor():
    client = object.__new__(ZenodoClient)
    records = [
        {"id": 1, "updated": "2026-01-02T00:00:00Z"},
        {"id": 2, "updated": "2026-01-01T00:00:00Z"},
    ]

    batch, should_stop = client._filter_records(
        records,
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    assert batch == [{"id": 1, "updated": "2026-01-02T00:00:00Z"}]
    assert should_stop is True
