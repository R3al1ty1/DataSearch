from enum import Enum


class AppConstants:
    PROJECT_NAME = "DataSearch"
    API_V1_STR = "/api"
    REDIS_URL = "redis://localhost:6379/0"
    EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"


class AppEnvironment(str, Enum):
    """Application environments."""
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"

class DBConnectArgs(dict):
    """Connect arguments for the database connection."""
    COMMAND_TIMEOUT = 60

class LogConfig(str, Enum):
    """Logging configuration."""
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

class ExternalAPIUrls:
    """External API base URLs."""
    HUGGINGFACE_DATASETS = "https://huggingface.co/api/datasets"
    KAGGLE_API = "https://www.kaggle.com/api/v1"
    DATAGOV_CATALOG_SEARCH = "https://catalog.data.gov/search"
    DATA_HEALTHCARE_GOV_CATALOG = "https://data.healthcare.gov/data.json"
    DATA_HEALTHCARE_GOV_DATASET = "https://data.healthcare.gov/dataset"
    ZENODO_RECORDS = "https://zenodo.org/api/records"
    WORLD_BANK_DDH = "https://ddh-openapi.worldbank.org"

class HuggingFaceTagPrefixes:
    """Prefixes for HuggingFace tags."""
    FILE_FORMATS = ['parquet', 'csv', 'json', 'text', 'arrow', 'webdataset']
    TASK_CATEGORIES = ['task_categories:', 'task_ids:']

class UserRole(str, Enum):
    """User roles for authorization."""
    USER = "user"
    ADMIN = "admin"

class AuthConstants:
    """Authentication and authorization constants."""
    # JWT
    JWT_ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 30
    REFRESH_TOKEN_EXPIRE_DAYS = 7

    # Password validation
    PASSWORD_MIN_LENGTH = 8
    PASSWORD_MAX_LENGTH = 128
    PASSWORD_REQUIRE_UPPERCASE = True
    PASSWORD_REQUIRE_LOWERCASE = True
    PASSWORD_REQUIRE_DIGIT = True
    PASSWORD_REQUIRE_SPECIAL = False

    # Rate limiting
    RATE_LIMIT_LOGIN_ATTEMPTS = 5
    RATE_LIMIT_LOGIN_WINDOW_SECONDS = 300  # 5 minutes

    # Redis auth database
    REDIS_AUTH_DB = 1

    # Redis key prefixes
    TOKEN_BLACKLIST_PREFIX = "blacklist:"
    RATE_LIMIT_PREFIX = "ratelimit:login:"

    # OAuth
    OAUTH_STATE_EXPIRE_SECONDS = 600  # 10 minutes
