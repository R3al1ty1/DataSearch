import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from lib.core.error_codes import ErrorCode
from lib.services.datasets.click_repository import ClickRepository
from lib.services.datasets.exceptions import (
    DatasetConflict,
    DatasetNotFound,
    SearchLogNotFound,
)
from lib.services.datasets.repository import DatasetRepository


class ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


def test_dataset_unique_integrity_error_maps_to_dataset_conflict():
    session = Mock()
    session.flush = AsyncMock(
        side_effect=IntegrityError(
            statement="",
            params={},
            orig=RuntimeError("idx_unique_external_dataset"),
        )
    )
    session.refresh = AsyncMock()
    repository = DatasetRepository(session)
    dataset = SimpleNamespace(source_name="kaggle", external_id="owner/dataset")

    with pytest.raises(DatasetConflict) as exc_info:
        asyncio.run(repository.create(dataset))

    assert exc_info.value.status_code == 409
    assert exc_info.value.error_code == ErrorCode.DATASET_CONFLICT
    assert exc_info.value.details == {
        "source_name": "kaggle",
        "external_id": "owner/dataset",
    }


def test_click_repository_raises_dataset_not_found_before_insert():
    session = Mock()
    session.execute = AsyncMock(return_value=ScalarResult(None))
    repository = ClickRepository(session)

    with pytest.raises(DatasetNotFound):
        asyncio.run(
            repository.record_click(
                user_id=uuid4(),
                dataset_id=uuid4(),
                search_log_id=None,
                position=0,
            )
        )

    session.add.assert_not_called()


def test_click_repository_raises_search_log_not_found_before_insert():
    session = Mock()
    session.execute = AsyncMock(
        side_effect=[
            ScalarResult(uuid4()),
            ScalarResult(None),
        ]
    )
    repository = ClickRepository(session)

    with pytest.raises(SearchLogNotFound) as exc_info:
        asyncio.run(
            repository.record_click(
                user_id=uuid4(),
                dataset_id=uuid4(),
                search_log_id=uuid4(),
                position=0,
            )
        )

    assert exc_info.value.error_code == ErrorCode.SEARCH_LOG_NOT_FOUND
    session.add.assert_not_called()
