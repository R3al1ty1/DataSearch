import re
from datetime import datetime

from lib.services.static_scores.constants import (
    DOCS_BASE,
    FORMAT_SCORES,
    FORMAT_UNKNOWN,
    LICENSE_ALIASES,
    LICENSE_TIER_SCORES,
    NON_COMMERCIAL_LICENSES,
    PERMISSIVE_LICENSES,
    SOCIAL_FLOOR,
    SOCIAL_WEIGHTS,
    STRONG_COPYLEFT_LICENSES,
    WEAK_COPYLEFT_LICENSES,
)


class DocsScorer:
    _BAD_NAME = re.compile(r"^(col|column|field|var|unnamed|x|y)[\s_:]*\d*$", re.IGNORECASE)

    def score(
        self,
        description: str | None,
        column_names: list[str] | None,
        tags: list[str] | None,
        row_count: int | None,
        total_size_bytes: int | None,
        source_updated_at: datetime | None,
    ) -> float:
        checklist = (
            0.30 * self._description_grade(description)
            + 0.30 * self._column_quality(column_names)
            + 0.15 * (1.0 if tags else 0.0)
            + 0.15 * (1.0 if (row_count is not None or total_size_bytes is not None) else 0.0)
            + 0.10 * (1.0 if source_updated_at is not None else 0.0)
        )
        return round(DOCS_BASE + (1 - DOCS_BASE) * checklist, 4)

    def _description_grade(self, text: str | None) -> float:
        n = len((text or "").strip())
        if n == 0:
            return 0.0
        if n < 50:
            return 0.3
        if n < 200:
            return 0.7

        return 1.0

    def _column_quality(self, names: list[str] | None) -> float:
        if not names:
            return 0.0
        good = sum(self._is_meaningful(n) for n in names) / len(names)
        if good < 0.3:
            return 0.3
        if good < 0.7:
            return 0.7
        return 1.0

    def _is_meaningful(self, name: str) -> bool:
        s = (name or "").strip()
        if len(s) < 3:
            return False
        if s.isdigit():
            return False
        if self._BAD_NAME.match(s):
            return False

        return True


class ReprScorer:
    def score(self, file_formats: list[str] | None) -> float:
        if not file_formats:
            return FORMAT_UNKNOWN
        return max(FORMAT_SCORES.get(fmt.lower(), FORMAT_UNKNOWN) for fmt in file_formats)


class LegalScorer:
    def score(self, license_field: str | list[str] | None) -> float:
        if not license_field:
            return LICENSE_TIER_SCORES["unknown"]
        licenses = license_field if isinstance(license_field, list) else [license_field]
        valid = [lic for lic in licenses if lic]
        if not valid:
            return LICENSE_TIER_SCORES["unknown"]
        return max(self._tier_score(lic) for lic in valid)

    def _tier_score(self, license_str: str) -> float:
        normalized = LICENSE_ALIASES.get(license_str.strip().lower(), license_str.strip().lower())
        if normalized in PERMISSIVE_LICENSES:
            return LICENSE_TIER_SCORES["permissive"]
        if normalized in WEAK_COPYLEFT_LICENSES:
            return LICENSE_TIER_SCORES["weak_copyleft"]
        if normalized in STRONG_COPYLEFT_LICENSES:
            return LICENSE_TIER_SCORES["strong_copyleft"]
        if normalized in NON_COMMERCIAL_LICENSES:
            return LICENSE_TIER_SCORES["non_commercial"]

        return LICENSE_TIER_SCORES["unknown"]


class SocialScorer:
    def score(
        self,
        norm_downloads: float | None,
        norm_views: float | None,
        norm_likes: float | None,
    ) -> float:
        signals = {
            k: v for k, v in {
                "downloads": norm_downloads,
                "views": norm_views,
                "likes": norm_likes,
            }.items() if v is not None
        }
        if not signals:
            return SOCIAL_FLOOR
        total_w = sum(SOCIAL_WEIGHTS[k] for k in signals)
        combined = sum(SOCIAL_WEIGHTS[k] / total_w * signals[k] for k in signals)
        return round(SOCIAL_FLOOR + (1 - SOCIAL_FLOOR) * combined, 4)
