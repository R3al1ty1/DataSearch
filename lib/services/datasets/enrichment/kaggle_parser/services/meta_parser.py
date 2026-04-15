import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import AsyncGenerator
from lib.core.container import container
from lib.services.datasets.schemas import KaggleMetaDatasetDTO
from ..models import MetaKaggleConsts, ParsingConsts
from ..utils import initialize_kaggle_api, get_csv_path

class MetaKaggleParser:
    """Handles Meta Kaggle CSV operations"""

    def __init__(self, cache_dir: str = MetaKaggleConsts.DEFAULT_CACHE_DIR):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._logger = container.logger
        self.api = initialize_kaggle_api()

    async def get_total_count(self) -> int:
        """Returns total datasets count in CSV."""
        csv_path = get_csv_path(self.cache_dir)

        if not csv_path.exists():
            return 0

        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            None,
            lambda: pd.read_csv(csv_path, usecols=[MetaKaggleConsts.ID_COLUMN])
        )

        return len(df)

    async def parse_csv_batches(
        self,
        batch_size: int = ParsingConsts.DEFAULT_BATCH_SIZE,
        min_last_activity: datetime | None = None
    ) -> AsyncGenerator[list[KaggleMetaDatasetDTO], None]:
        """Parses Datasets.csv and yields batches."""
        csv_path = get_csv_path(self.cache_dir)

        if not csv_path.exists():
            self._logger.error(f"CSV not found at {csv_path}")
            return

        df = await self._load_csv(csv_path)
        df = self._filter_by_date(df=df, min_date=min_last_activity)
        df = self._sort_by_activity(df=df)

        async for batch in self._yield_batches(df=df, batch_size=batch_size):
            yield batch

    async def check_for_updates(self, last_download: datetime) -> bool:
        """Checks if Meta Kaggle has newer version."""
        loop = asyncio.get_event_loop()

        try:
            dataset_obj = await loop.run_in_executor(
                None,
                lambda: self.api.dataset_view(MetaKaggleConsts.DATASET_REF)
            )

            if (
                not hasattr(dataset_obj, 'lastUpdated')
                or not dataset_obj.lastUpdated
            ):
                self._logger.warning("No lastUpdated field in Meta Kaggle")
                return False

            kaggle_updated = self._normalize_timezone(
                dt=dataset_obj.lastUpdated, reference=last_download
            )
            is_updated = kaggle_updated > last_download

            self._log_update_status(
                is_updated=is_updated,
                kaggle_date=kaggle_updated,
                local_date=last_download
            )

            return is_updated

        except Exception as e:
            self._logger.error(f"Failed to check updates: {e}")
            return False

    async def download_if_needed(self, force: bool = False) -> Path:
        """Downloads Meta Kaggle CSV if not cached."""
        csv_path = get_csv_path(self.cache_dir)

        if csv_path.exists() and not force:
            self._logger.info(f"CSV cached at {csv_path}")
            return csv_path

        await self._download_dataset()
        return csv_path

    async def _load_csv(self, csv_path: Path) -> pd.DataFrame:
        """Loads and parses CSV file."""
        self._logger.info(f"Loading CSV from {csv_path}")

        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            None,
            lambda: pd.read_csv(
                csv_path,
                parse_dates=MetaKaggleConsts.DATE_COLUMNS,
                low_memory=False
            )
        )

        self._logger.info(f"Loaded {len(df)} datasets")
        return df

    def _filter_by_date(
        self,
        df: pd.DataFrame,
        min_date: datetime | None
    ) -> pd.DataFrame:
        """Filters dataframe by minimum activity date."""
        if not min_date:
            return df

        filtered = df[
            (pd.isna(df['LastActivityDate'])) |
            (df['LastActivityDate'] >= min_date)
        ]

        self._logger.info(f"Filtered to {len(filtered)} datasets after {min_date}")
        return filtered

    def _sort_by_activity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sorts by LastActivityDate descending."""
        return df.sort_values('LastActivityDate', ascending=False, na_position='last')

    def _log_update_status(
        self,
        is_updated: bool,
        kaggle_date: datetime,
        local_date: datetime
    ) -> None:
        """Logs update check result."""
        if is_updated:
            self._logger.info(
                f"Updates available: {kaggle_date} > {local_date}"
            )
        else:
            self._logger.info(
                f"Up to date: {kaggle_date} <= {local_date}"
            )

    def _normalize_timezone(
        self, dt: datetime, reference: datetime
    ) -> datetime:
        """Normalizes timezone of datetime to match reference."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=reference.tzinfo)
        return dt

    async def _yield_batches(
        self,
        df: pd.DataFrame,
        batch_size: int
    ) -> AsyncGenerator[list[KaggleMetaDatasetDTO], None]:
        """Yields batches of DTOs from dataframe."""
        total = len(df)
        processed = 0

        for i in range(0, total, batch_size):
            batch_df = df.iloc[i:i + batch_size]
            batch_dtos = self._convert_to_dtos(df=batch_df)

            if batch_dtos:
                processed += len(batch_dtos)
                batch_num = i // batch_size + 1
                self._logger.info(
                    f"Batch {batch_num}: {len(batch_dtos)} datasets "
                    f"(total: {processed}/{total})"
                )
                yield batch_dtos

            await asyncio.sleep(ParsingConsts.CHUNK_DELAY_SECONDS)

    async def _download_dataset(self) -> None:
        """Downloads Meta Kaggle dataset via API."""
        self._logger.info(f"Downloading Meta Kaggle to {self.cache_dir}")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self.api.dataset_download_files(
                MetaKaggleConsts.DATASET_REF,
                path=str(self.cache_dir),
                unzip=True,
                quiet=False
            )
        )

        self._logger.info("Download completed")

    def _convert_to_dtos(self, df: pd.DataFrame) -> list[KaggleMetaDatasetDTO]:
        """Converts dataframe rows to DTOs."""
        dtos = []

        for _, row in df.iterrows():
            try:
                dto = KaggleMetaDatasetDTO(**row.to_dict())
                dtos.append(dto)
            except Exception as e:
                dataset_id = row.get(MetaKaggleConsts.ID_COLUMN, 'unknown')
                self._logger.error(f"Failed to parse dataset {dataset_id}: {e}")

        return dtos
