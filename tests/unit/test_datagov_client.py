from datetime import datetime, timezone

from lib.services.datasets.enrichment.datagov_parser.client import DataGovClient


def test_datagov_client_caps_page_size():
    client = object.__new__(DataGovClient)

    assert client._page_size(500) == 100


def test_datagov_client_extracts_results_payload():
    client = object.__new__(DataGovClient)
    payload = {"results": [{"identifier": "one"}, {"identifier": "two"}, "bad"]}

    assert client._extract_records(payload) == [
        {"identifier": "one"},
        {"identifier": "two"},
    ]


def test_datagov_client_reads_next_cursor():
    client = object.__new__(DataGovClient)

    assert client._next_cursor({"after": "cursor"}) == "cursor"
    assert client._next_cursor({"after": ""}) is None


def test_datagov_client_stops_when_record_is_older_than_cursor():
    client = object.__new__(DataGovClient)
    records = [
        {"identifier": "one", "last_harvested_date": "2026-01-02T00:00:00"},
        {"identifier": "two", "last_harvested_date": "2026-01-01T00:00:00"},
    ]

    batch, should_stop = client._filter_records(
        records,
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    assert batch == [
        {"identifier": "one", "last_harvested_date": "2026-01-02T00:00:00"}
    ]
    assert should_stop is True
