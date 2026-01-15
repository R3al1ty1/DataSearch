import logging
from functools import cached_property

from lib.core.config import Settings
from lib.core.database import DatabaseManager
from lib.core.logger import LoggerManager
from lib.services.ml.embedder import EmbeddingService


class AppContainer:
    """Dependency Injection root container with lazy initialization."""

    @property
    def logger(self) -> logging.Logger:
        """Application logger."""
        return self.logger_manager.get_logger()

    @cached_property
    def settings(self) -> Settings:
        """Application settings."""
        return Settings()

    @cached_property
    def logger_manager(self) -> LoggerManager:
        """Logger manager."""
        return LoggerManager()

    @cached_property
    def db(self) -> DatabaseManager:
        """Database manager."""
        return DatabaseManager(
            dsn=self.settings.SQLALCHEMY_DATABASE_URI,
            environment=self.settings.ENVIRONMENT,
            logger=self.logger
        )

    @cached_property
    def embedder(self) -> EmbeddingService:
        """ML embedding service."""
        return EmbeddingService(
            model_name=self.settings.EMBEDDING_MODEL,
            logger=self.logger
        )

    @cached_property
    def hf_client(self):
        """HuggingFace API client."""
        from lib.services.enrichment.hf_parser import HuggingFaceClient
        return HuggingFaceClient()

    @cached_property
    def kaggle_client(self):
        """Kaggle API client."""
        from lib.services.enrichment.kaggle_parser import KaggleClient
        return KaggleClient()

    @cached_property
    def dataset_repo(self):
        """Dataset repository."""
        from lib.repositories import DatasetRepository
        return DatasetRepository()

    @cached_property
    def enrichment_log_repo(self):
        """Enrichment log repository."""
        from lib.repositories import EnrichmentLogRepository
        return EnrichmentLogRepository()

    @cached_property
    def hf_processor(self):
        """HuggingFace processor."""
        from lib.services.enrichment.hf_parser.processor import HFProcessor
        return HFProcessor(
            hf_client=self.hf_client,
            dataset_repo=self.dataset_repo
        )

    @cached_property
    def kaggle_processor(self):
        """Kaggle processor."""
        from lib.services.enrichment.kaggle_parser.processor import KaggleProcessor
        return KaggleProcessor(
            kaggle_client=self.kaggle_client,
            dataset_repo=self.dataset_repo,
            log_repo=self.enrichment_log_repo
        )

    @cached_property
    def embedding_processor(self):
        """Embedding processor."""
        from lib.services.ml.embedding_processor import EmbeddingProcessor
        return EmbeddingProcessor(
            dataset_repo=self.dataset_repo,
            embedder=self.embedder
        )


container = AppContainer()
