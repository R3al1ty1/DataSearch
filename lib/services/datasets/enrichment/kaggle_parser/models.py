class MetaKaggleConsts:
    """Configuration for Meta Kaggle dataset."""

    DATASET_REF = "kaggle/meta-kaggle"
    CSV_FILENAME = "Datasets.csv"

    DATE_COLUMNS = ["CreationDate", "LastActivityDate"]
    ID_COLUMN = "Id"

    DEFAULT_CACHE_DIR = "./data/meta_kaggle"

class APIConsts:
    """Configuration for Kaggle API client."""

    DEFAULT_THROTTLE_DELAY = 1.0
    DEFAULT_PAGE_SIZE = 20

    MAX_RETRY_ATTEMPTS = 3
    RETRY_MIN_WAIT = 2
    RETRY_MAX_WAIT = 10

class ParsingConsts:
    """Configuration for CSV parsing."""

    DEFAULT_BATCH_SIZE = 1000
    CHUNK_DELAY_SECONDS = 0.01
