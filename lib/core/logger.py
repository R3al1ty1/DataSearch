import logging
import sys

from lib.core.constants import LogConfig


class LoggerManager:
    """Logger manager for setting up global logging configuration."""
    def __init__(self):
        """
        Initializes the global logging configuration once upon instantiation.
        """
        root = logging.getLogger()
        if not root.handlers:
            logging.basicConfig(
                stream=sys.stdout,
                level=logging.INFO,
                format=LogConfig.FORMAT.value
            )

        self._logger = logging.getLogger()

    def get_logger(self) -> logging.Logger:
        """Dependency Provider for FastAPI."""
        return self._logger
