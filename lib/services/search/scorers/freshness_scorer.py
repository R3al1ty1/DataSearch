import math
from datetime import datetime, timezone


class FreshnessScorer:
    """Computes a recency score based on source_updated_at using exponential decay."""

    _DEFAULT_SCORE = 0.5

    def __init__(self, halflife_days: int = 365):
        self._decay_rate = math.log(2) / halflife_days

    def score(self, source_updated_at: datetime | None) -> float:
        """Returns freshness score in (0.0, 1.0]. None → 0.5 (neutral)."""
        if source_updated_at is None:
            return self._DEFAULT_SCORE
        now = datetime.now(timezone.utc)
        age_days = max(0.0, (now - source_updated_at).total_seconds() / 86400)
        return round(math.exp(-self._decay_rate * age_days), 4)
