import os
from pathlib import Path

from lib.core.container import container
from .models import MetaKaggleConsts


def get_csv_path(cache_dir: Path) -> Path:
    """Returns path to Datasets.csv."""
    return cache_dir / MetaKaggleConsts.CSV_FILENAME


def initialize_kaggle_api():
    """Initializes and authenticates Kaggle API."""
    settings = container.settings

    if settings.KAGGLE_USERNAME and settings.KAGGLE_KEY:
        os.environ['KAGGLE_USERNAME'] = settings.KAGGLE_USERNAME
        os.environ['KAGGLE_KEY'] = settings.KAGGLE_KEY
        container.logger.info(f"Set KAGGLE_USERNAME={settings.KAGGLE_USERNAME[:5]}...")

    else:
        container.logger.warning("No Kaggle credentials in settings!")

    _patch_kaggle_client()

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    container.logger.info("Kaggle API authenticated")

    return api


def _patch_kaggle_client():
    """Patches KaggleClient to fix User-Agent bug in kagglesdk."""
    from kagglesdk.kaggle_client import KaggleClient

    original_init = KaggleClient.__init__

    def patched_init(self, *args, **kwargs):
        kwargs.pop('user_agent', None)
        original_init(self, *args, **kwargs)

    KaggleClient.__init__ = patched_init
