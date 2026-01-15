from pydantic import BaseModel, Field


class SourceStats(BaseModel):
    """Statistics for a specific data source."""
    source: str = Field(..., description="Data source name")
    total: int = Field(..., description="Total datasets count")
    minimal: int = Field(..., description="Datasets with minimal info")
    pending: int = Field(..., description="Datasets pending enrichment")
    enriching: int = Field(..., description="Datasets currently enriching")
    enriched: int = Field(..., description="Fully enriched datasets")
    failed: int = Field(..., description="Failed enrichment datasets")
    skipped: int = Field(0, description="Skipped datasets")

    @property
    def enrichment_progress(self) -> float:
        """Calculate enrichment progress percentage."""
        if self.total == 0:
            return 0.0
        return (self.enriched / self.total) * 100


class EnrichmentStageStats(BaseModel):
    """Statistics for enrichment stage and result."""
    stage: str = Field(..., description="Enrichment stage")
    result: str = Field(..., description="Enrichment result")
    count: int = Field(..., description="Number of attempts")
    avg_duration_ms: float | None = Field(
        None,
        description="Average duration in milliseconds"
    )


class ErrorStats(BaseModel):
    """Statistics for error types."""
    error_type: str = Field(..., description="Error type")
    count: int = Field(..., description="Number of occurrences")
