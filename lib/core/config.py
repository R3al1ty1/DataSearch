from pydantic import PostgresDsn, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

from lib.core.constants import AppEnvironment


class Settings(BaseSettings):
    """Settings for the application."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
        case_sensitive=True
    )

    PROJECT_NAME: str = "DataSearch"
    API_V1_STR: str = "/api"
    ENVIRONMENT: AppEnvironment = AppEnvironment.LOCAL
    DEBUG: bool = False

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str

    REDIS_URL: RedisDsn = "redis://localhost:6379/0"

    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"

    # External API tokens
    HF_TOKEN: str | None = None
    KAGGLE_USERNAME: str | None = None
    KAGGLE_KEY: str | None = None

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
