from uuid import UUID
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl, ConfigDict


class HFDatasetDTO(BaseModel):
    """DTO for HuggingFace API response."""
    id: str = Field(..., description="Repo ID")
    sha: Optional[str] = None

    last_modified: Optional[datetime] = Field(default=None, alias="lastModified")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")

    downloads: int = 0
    likes: int = 0
    tags: List[str] = Field(default_factory=list)
    description: Optional[str] = None

    card_data: Optional[dict] = Field(default=None, alias="cardData")
    dataset_info: Optional[dict] = Field(default=None, alias="datasetInfo")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @property
    def title(self) -> str:
        if self.card_data and "pretty_name" in self.card_data:
            return str(self.card_data["pretty_name"])
        return self.id.split("/")[-1]

    @property
    def license(self) -> Optional[str]:
        if self.card_data and "license" in self.card_data:
            lic = self.card_data["license"]
            if isinstance(lic, list) and lic:
                return str(lic[0])
            if isinstance(lic, str):
                return lic
        for tag in self.tags:
            if tag.startswith("license:"):
                return tag.split(":", 1)[1]
        return None

    def get_update_time(self) -> datetime:
        """Get dataset update time safely."""
        if self.last_modified:
            return self.last_modified
        if self.created_at:
            return self.created_at
        return datetime.now(timezone.utc)


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=200, description="Natural language search query")
    limit: int = Field(10, ge=1, le=50, description="Number of results to return")
    offset: int = Field(0, ge=0, description="Pagination offset")


class DatasetItem(BaseModel):
    """Модель датасета в поисковой выдаче"""
    id: UUID
    source_name: str
    external_id: str
    title: str
    description: Optional[str] = None
    url: HttpUrl
    score: float = Field(..., description="Relevance score (0.0 - 1.0)")
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class SearchResponse(BaseModel):
    items: List[DatasetItem]
    total: int
    execution_time_ms: float
