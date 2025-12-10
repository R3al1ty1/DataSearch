import logging

from lib.core.config import Settings
from lib.core.database import DatabaseManager
from lib.core.logger import LoggerManager


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

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def db(self) -> DatabaseManager:
        return self._db_manager


container = AppContainer()
