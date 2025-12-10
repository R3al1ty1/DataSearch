import logging
import sys

from lib.core.constants import LogConfig


def setup_logging() -> logging.Logger:
    """Setups logger for service."""
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format=LogConfig.FORMAT
    )

    logger = logging.getLogger()

    return logger
