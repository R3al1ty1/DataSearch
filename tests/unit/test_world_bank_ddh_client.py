import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import httpx

from lib.services.datasets.enrichment.world_bank_ddh_parser.client import (
    WorldBankDDHClient,
)


def test_world_bank_ddh_client_caps_page_size():
    client = object.__new__(WorldBankDDHClient)

    assert client._page_size(500) == 100


def test_world_bank_ddh_client_extracts_data_payload():
    client = object.__new__(WorldBankDDHClient)
    payload = {"data": [{"dataset_unique_id": "one"}, "bad", {"dataset_unique_id": "two"}]}

    assert client._extract_records(payload) == [
        {"dataset_unique_id": "one"},
        {"dataset_unique_id": "two"},
    ]


def test_world_bank_ddh_client_reads_updated_timestamp():
    client = object.__new__(WorldBankDDHClient)

    assert client._record_updated_at(
        {"modified_on": "2026-06-03T18:13:28+00:00"}
    ) == datetime(2026, 6, 3, 18, 13, 28, tzinfo=timezone.utc)


def test_world_bank_ddh_client_uses_last_updated_fallback():
    client = object.__new__(WorldBankDDHClient)

    assert client._record_updated_at(
        {"last_updated_date": "2026-05-13T20:23:35+00:00"}
    ) == datetime(2026, 5, 13, 20, 23, 35, tzinfo=timezone.utc)


def test_world_bank_ddh_client_skips_schema_for_download_only_archive():
    client = object.__new__(WorldBankDDHClient)

    assert client._supports_resource_metadata(
        {
            "resource_unique_id": "DR0096141",
            "format": "ZIP",
            "distribution": {
                "distribution_format": "zip",
                "is_directory": False,
            },
        }
    ) is False


def test_world_bank_ddh_client_allows_schema_for_tabular_resources():
    client = object.__new__(WorldBankDDHClient)

    assert client._supports_resource_metadata({"format": "CSV"}) is True
    assert client._supports_resource_metadata(
        {"distribution": {"distribution_format": "application/json"}}
    ) is True


def test_world_bank_ddh_client_skips_unavailable_resource_metadata():
    client = object.__new__(WorldBankDDHClient)
    client.headers = {"User-Agent": "test"}
    client.timeout = 30
    client._logger = Mock()
    request = httpx.Request(
        "GET",
        "https://ddh-openapi.worldbank.org/resources/DR0096141/metadata",
    )
    response = httpx.Response(400, request=request)
    http_client = AsyncMock()
    http_client.get.return_value = response

    assert asyncio.run(client._fetch_resource_metadata(http_client, "DR0096141")) is None
