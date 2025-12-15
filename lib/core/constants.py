from enum import Enum


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
