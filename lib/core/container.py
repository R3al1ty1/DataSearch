import logging

from lib.core.config import Settings
from lib.core.database import DatabaseManager
from lib.core.logger import LoggerManager
from lib.services.ml.embedder import EmbeddingService


class AppContainer:
    """Dependency Injection root container."""
    def __init__(self):
        self._settings = Settings()
        self._logger = LoggerManager()

        self._db_manager = DatabaseManager(
            dsn=self._settings.SQLALCHEMY_DATABASE_URI,
            environment=self._settings.ENVIRONMENT,
            logger=self._logger.get_logger()
        )

        self._embedding_service = None
        self._hf_client = None
        self._kaggle_client = None

        self._dataset_repo = None
        self._enrichment_log_repo = None

        self._hf_processor = None
        self._kaggle_processor = None

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def logger(self) -> logging.Logger:
        return self._logger.get_logger()

    @property
    def logger_manager(self):
        return self._logger

    @property
    def db(self) -> DatabaseManager:
        return self._db_manager

    @property
    def embedder(self) -> EmbeddingService:
        if self._embedding_service is None:
            self._embedding_service = EmbeddingService(
                model_name=self._settings.EMBEDDING_MODEL,
                logger=self._logger.get_logger()
            )
        return self._embedding_service

    @property
    def hf_client(self):
        if self._hf_client is None:
            from lib.services.enrichment.hf_parser import HuggingFaceClient
            self._hf_client = HuggingFaceClient()
        return self._hf_client

    @property
    def kaggle_client(self):
        if self._kaggle_client is None:
            from lib.services.enrichment.kaggle_parser import KaggleClient
            self._kaggle_client = KaggleClient()
        return self._kaggle_client

    @property
    def dataset_repo(self):
        if self._dataset_repo is None:
            from lib.repositories import DatasetRepository
            self._dataset_repo = DatasetRepository()
        return self._dataset_repo

    @property
    def enrichment_log_repo(self):
        if self._enrichment_log_repo is None:
            from lib.repositories import EnrichmentLogRepository
            self._enrichment_log_repo = EnrichmentLogRepository()
        return self._enrichment_log_repo

    @property
    def hf_processor(self):
        if self._hf_processor is None:
            from lib.services.enrichment.hf_parser.processor import HFProcessor
            self._hf_processor = HFProcessor(
                hf_client=self.hf_client,
                dataset_repo=self.dataset_repo
            )
        return self._hf_processor

    @property
    def kaggle_processor(self):
        if self._kaggle_processor is None:
            from lib.services.enrichment.kaggle_parser.processor import (
                KaggleProcessor
            )
            self._kaggle_processor = KaggleProcessor(
                kaggle_client=self.kaggle_client,
                dataset_repo=self.dataset_repo,
                log_repo=self.enrichment_log_repo
            )
        return self._kaggle_processor


container = AppContainer()
