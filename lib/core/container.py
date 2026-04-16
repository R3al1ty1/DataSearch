import logging
from functools import cached_property

from lib.core.config import Settings
from lib.core.database import DatabaseManager
from lib.core.logger import LoggerManager
from lib.services.datasets.ml.embedder import EmbeddingService


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
        from lib.services.datasets.enrichment.hf_parser import HuggingFaceClient
        return HuggingFaceClient()

    @cached_property
    def kaggle_client(self):
        """Kaggle API client."""
        from lib.services.datasets.enrichment.kaggle_parser import KaggleClient
        return KaggleClient()

    @cached_property
    def dataset_repo(self):
        """Dataset repository."""
        from lib.services.datasets.repository import DatasetRepository
        return DatasetRepository()

    @cached_property
    def search_log_repo(self):
        """Search log repository."""
        from lib.services.datasets.search_log_repository import SearchLogRepository
        return SearchLogRepository()

    @cached_property
    def search_service(self):
        """Search service."""
        from lib.services.search import SearchService
        return SearchService(
            dataset_repo=self.dataset_repo,
            search_log_repo=self.search_log_repo,
            embedder=self.embedder,
            logger=self.logger,
        )

    @cached_property
    def enrichment_log_repo(self):
        """Enrichment log repository."""
        from lib.services.datasets.repository import EnrichmentLogRepository
        return EnrichmentLogRepository()

    @cached_property
    def hf_processor(self):
        """HuggingFace processor."""
        from lib.services.datasets.enrichment.hf_parser.processor import HFProcessor
        return HFProcessor(
            hf_client=self.hf_client,
            dataset_repo=self.dataset_repo
        )

    @cached_property
    def kaggle_processor(self):
        """Kaggle processor."""
        from lib.services.datasets.enrichment.kaggle_parser.processor import (
            KaggleProcessor,
        )
        return KaggleProcessor(
            kaggle_client=self.kaggle_client,
            dataset_repo=self.dataset_repo,
            log_repo=self.enrichment_log_repo
        )

    @cached_property
    def embedding_processor(self):
        """Embedding processor."""
        from lib.services.datasets.ml.embedding_processor import EmbeddingProcessor
        return EmbeddingProcessor(
            dataset_repo=self.dataset_repo,
            embedder=self.embedder
        )

    @cached_property
    def redis_auth(self):
        """Redis manager for authentication."""
        from lib.core.redis_auth import RedisAuthManager
        return RedisAuthManager(
            redis_url=self.settings.REDIS_AUTH_URL,
            logger=self.logger
        )

    @cached_property
    def user_repo(self):
        """User repository."""
        from lib.auth.repository import UserRepository
        return UserRepository()

    @cached_property
    def security_event_repo(self):
        """Security event repository."""
        from lib.auth.repository import SecurityEventRepository
        return SecurityEventRepository()

    @cached_property
    def token_service(self):
        """Token service."""
        from lib.auth.services.token_service import TokenService
        return TokenService(
            redis_manager=self.redis_auth,
            settings=self.settings,
            logger=self.logger
        )

    @cached_property
    def rate_limit_service(self):
        """Rate limit service."""
        from lib.auth.services.rate_limit_service import RateLimitService
        return RateLimitService(
            redis_manager=self.redis_auth,
            logger=self.logger
        )

    @cached_property
    def auth_service(self):
        """Auth service."""
        from lib.auth.services.auth_service import AuthService
        return AuthService(
            user_repo=self.user_repo,
            security_event_repo=self.security_event_repo,
            token_service=self.token_service,
            rate_limit_service=self.rate_limit_service,
            settings=self.settings,
            logger=self.logger
        )

    @cached_property
    def oauth_service(self):
        """OAuth service."""
        from lib.auth.services.oauth_service import OAuthService
        return OAuthService(
            user_repo=self.user_repo,
            security_event_repo=self.security_event_repo,
            token_service=self.token_service,
            redis_manager=self.redis_auth,
            settings=self.settings,
            logger=self.logger
        )


container = AppContainer()
