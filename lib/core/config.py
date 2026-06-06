from pydantic import PostgresDsn, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

from lib.core.constants import AppConstants, AppEnvironment, AuthConstants


class Settings(BaseSettings):
    """Settings for the application."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
        case_sensitive=True
    )

    PROJECT_NAME: str = AppConstants.PROJECT_NAME
    API_V1_STR: str = AppConstants.API_V1_STR
    ENVIRONMENT: AppEnvironment = AppEnvironment.LOCAL
    DEBUG: bool = False

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str

    REDIS_URL: RedisDsn = AppConstants.REDIS_URL

    EMBEDDING_MODEL: str = AppConstants.EMBEDDING_MODEL

    # External API tokens
    HF_TOKEN: str | None = None
    KAGGLE_USERNAME: str | None = None
    KAGGLE_KEY: str | None = None
    ZENODO_ACCESS_TOKEN: str | None = None

    # JWT settings
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = AuthConstants.JWT_ALGORITHM
    ACCESS_TOKEN_EXPIRE_MINUTES: int = AuthConstants.ACCESS_TOKEN_EXPIRE_MINUTES
    REFRESH_TOKEN_EXPIRE_DAYS: int = AuthConstants.REFRESH_TOKEN_EXPIRE_DAYS

    # OAuth - Google (optional)
    GOOGLE_CLIENT_ID: str | None = None
    GOOGLE_CLIENT_SECRET: str | None = None

    # OAuth - Yandex (optional)
    YANDEX_CLIENT_ID: str | None = None
    YANDEX_CLIENT_SECRET: str | None = None

    # OAuth callback base URL (this API) and frontend redirect URL
    OAUTH_CALLBACK_BASE_URL: str = "http://localhost:8000"
    FRONTEND_URL: str = "http://localhost:3000"

    # Ranking
    RANKING_STRATEGY: str = "v1_hybrid"
    FRESHNESS_HALFLIFE_DAYS: int = 365

    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """Database connection URI."""
        return str(PostgresDsn.build(
            scheme="postgresql+asyncpg",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            path=self.POSTGRES_DB,
        ))

    @property
    def REDIS_AUTH_URL(self) -> str:
        """Redis URL for authentication (database 1)."""
        base_url = str(self.REDIS_URL).rsplit('/', 1)[0]
        return f"{base_url}/{AuthConstants.REDIS_AUTH_DB}"
