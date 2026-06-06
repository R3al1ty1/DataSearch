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

    def uow(self):
        return self.db.uow()

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
    def data_healthcare_client(self):
        """Data.Healthcare.gov API client."""
        from lib.services.datasets.enrichment.healthcare_parser import (
            DataHealthcareClient,
        )
        return DataHealthcareClient()

    @cached_property
    def zenodo_client(self):
        """Zenodo API client."""
        from lib.services.datasets.enrichment.zenodo_parser import ZenodoClient
        return ZenodoClient()

    @cached_property
    def freshness_scorer(self):
        """Freshness scorer for relevance ranking."""
        from lib.services.search.scorers.freshness_scorer import FreshnessScorer
        return FreshnessScorer(halflife_days=self.settings.FRESHNESS_HALFLIFE_DAYS)

    @cached_property
    def relevance_ranker(self):
        """Relevance ranker with configurable strategy."""
        from lib.services.search.scorers.relevance_ranker import RelevanceRanker
        return RelevanceRanker(
            freshness_scorer=self.freshness_scorer,
            strategy=self.settings.RANKING_STRATEGY,
        )

    @cached_property
    def search_service(self):
        """Search service."""
        from lib.services.search import SearchService
        return SearchService(
            embedder=self.embedder,
            ranker=self.relevance_ranker,
            logger=self.logger,
        )

    @cached_property
    def hf_processor(self):
        """HuggingFace processor."""
        from lib.services.datasets.enrichment.hf_parser.processor import HFProcessor
        return HFProcessor(
            hf_client=self.hf_client
        )

    @cached_property
    def kaggle_processor(self):
        """Kaggle processor."""
        from lib.services.datasets.enrichment.kaggle_parser.processor import (
            KaggleProcessor,
        )
        return KaggleProcessor(
            kaggle_client=self.kaggle_client
        )

    @cached_property
    def data_healthcare_processor(self):
        """Data.Healthcare.gov processor."""
        from lib.services.datasets.enrichment.healthcare_parser import (
            DataHealthcareProcessor,
        )
        return DataHealthcareProcessor(
            client=self.data_healthcare_client,
        )

    @cached_property
    def zenodo_processor(self):
        """Zenodo processor."""
        from lib.services.datasets.enrichment.zenodo_parser import ZenodoProcessor
        return ZenodoProcessor(
            client=self.zenodo_client,
        )

    @cached_property
    def static_score_service(self):
        """Static score service."""
        from lib.services.static_scores import StaticScoreService
        return StaticScoreService(
            logger=self.logger,
        )

    @cached_property
    def embedding_processor(self):
        """Embedding processor."""
        from lib.services.datasets.ml.embedding_processor import EmbeddingProcessor
        return EmbeddingProcessor(
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
            token_service=self.token_service,
            logger=self.logger
        )


container = AppContainer()
