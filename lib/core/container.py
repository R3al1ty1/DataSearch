import logging

from lib.core.config import Settings
from lib.core.database import DatabaseManager
from lib.core.logger import LoggerManager
from lib.services.ml.embedder import EmbeddingService
from lib.services.enrichment.hf_parser import HuggingFaceClient
from lib.services.enrichment.kaggle_parser import KaggleClient


class AppContainer:
    """Dependency Injection root container."""
    def __init__(self):
        self._settings = Settings()
        self._logger = LoggerManager()

        self._db_manager = DatabaseManager(
            dsn=self._settings.SQLALCHEMY_DATABASE_URI,
            environment=self._settings.ENVIRONMENT,
            logger=self._logger
        )

        self._embedding_service = EmbeddingService()
        self._hf_client = HuggingFaceClient()
        self._kaggle_client = KaggleClient()

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def db(self) -> DatabaseManager:
        return self._db_manager

    @property
    def embedder(self) -> EmbeddingService:
        return self._embedding_service

    @property
    def hf_client(self) -> HuggingFaceClient:
        return self._hf_client

    @property
    def kaggle_client(self) -> KaggleClient:
        return self._kaggle_client


container = AppContainer()
