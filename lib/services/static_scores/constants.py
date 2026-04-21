FORMAT_SCORES: dict[str, float] = {
    "parquet": 1.0,
    "csv": 0.9,
    "json": 0.9,
    "jsonl": 0.9,
    "arrow": 0.9,
    "feather": 0.9,
    "tsv": 0.8,
    "xls": 0.6,
    "xlsx": 0.6,
    "xml": 0.4,
    "html": 0.4,
    "pdf": 0.2,
    "doc": 0.2,
    "docx": 0.2,
}
FORMAT_UNKNOWN = 0.3

PERMISSIVE_LICENSES = frozenset({
    "mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause",
    "cc-by-3.0", "cc-by-4.0", "cc0-1.0",
    "cdla-permissive-2.0", "odc-by-1.0",
})
WEAK_COPYLEFT_LICENSES = frozenset({
    "mpl-2.0", "lgpl-2.1", "lgpl-3.0",
    "cc-by-sa-3.0", "cc-by-sa-4.0",
    "odbl-1.0", "cdla-sharing-1.0",
})
STRONG_COPYLEFT_LICENSES = frozenset({
    "gpl-2.0", "gpl-3.0", "agpl-3.0",
})
NON_COMMERCIAL_LICENSES = frozenset({
    "cc-by-nc-4.0", "cc-by-nc-sa-4.0", "cc-by-nc-nd-4.0",
})

LICENSE_ALIASES: dict[str, str] = {
    "apache": "apache-2.0",
    "gpl": "gpl-3.0",
    "lgpl": "lgpl-3.0",
    "mit license": "mit",
    "cc by 4.0": "cc-by-4.0",
    "cc0": "cc0-1.0",
    "public domain": "cc0-1.0",
    "bsd": "bsd-3-clause",
    "odbl": "odbl-1.0",
}

LICENSE_TIER_SCORES: dict[str, float] = {
    "permissive": 1.0,
    "weak_copyleft": 0.8,
    "strong_copyleft": 0.6,
    "non_commercial": 0.4,
    "unknown": 0.3,
}

SOCIAL_WEIGHTS: dict[str, float] = {
    "downloads": 0.5,
    "views": 0.3,
    "likes": 0.2,
}
SOCIAL_PERCENTILE_LOW = 5
SOCIAL_PERCENTILE_HIGH = 95
SOCIAL_FLOOR = 0.40

SOURCE_AVAILABLE_SIGNALS: dict[str, frozenset[str]] = {
    "huggingface": frozenset({"downloads", "likes"}),
    "kaggle": frozenset({"downloads", "views", "likes"}),
}
ALL_SIGNALS = frozenset({"downloads", "views", "likes"})

COBB_DOUGLAS_WEIGHTS: dict[str, float] = {
    "docs": 0.40,
    "repr": 0.15,
    "social": 0.25,
    "legal": 0.20,
}
DOCS_BASE = 0.15
