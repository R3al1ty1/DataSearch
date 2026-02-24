from uuid import UUID
from datetime import datetime, timezone
from pydantic import BaseModel, Field, HttpUrl, ConfigDict


class HFDatasetDTO(BaseModel):
    """DTO for HuggingFace API response."""
    id: str = Field(..., description="Repo ID")
    sha: str | None = None

    last_modified: datetime | None = Field(default=None, alias="lastModified")
    created_at: datetime | None = Field(default=None, alias="createdAt")

    downloads: int = 0
    likes: int = 0
    tags: list[str] = Field(default_factory=list)
    description: str | None = None

    card_data: dict | None = Field(default=None, alias="cardData")
    dataset_info: dict | None = Field(default=None, alias="datasetInfo")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @property
    def title(self) -> str:
        if self.card_data and "pretty_name" in self.card_data:
            return str(self.card_data["pretty_name"])
        return self.id.split("/")[-1]

    @property
    def license(self) -> str | None:
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


class KaggleMetaDatasetDTO(BaseModel):
    """DTO for Meta Kaggle CSV (Datasets.csv) - minimal metadata."""
    Id: int = Field(..., description="Kaggle dataset ID")
    CreatorUserId: int | None = None
    OwnerUserId: int | None = None
    OwnerOrganizationId: int | None = None
    CurrentDatasetVersionId: int | None = None
    CurrentDatasourceVersionId: int | None = None
    ForumId: int | None = None
    Type: str | None = None
    CreationDate: datetime | None = None
    LastActivityDate: datetime | None = None
    TotalViews: int = 0
    TotalDownloads: int = 0
    TotalVotes: int = 0
    TotalKernels: int = 0

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @property
    def external_id(self) -> str:
        """Returns string representation of dataset ID."""
        return str(self.Id)

    def get_update_time(self) -> datetime:
        """Get dataset update time safely."""
        if self.LastActivityDate:
            return self.LastActivityDate
        if self.CreationDate:
            return self.CreationDate
        return datetime.now(timezone.utc)


class KaggleEnrichedDatasetDTO(BaseModel):
    """DTO for enriched Kaggle dataset from API (detailed metadata)."""
    ref: str = Field(..., description="Dataset reference (owner/dataset-name)")
    title: str
    subtitle: str | None = None
    creatorName: str | None = None
    totalBytes: int = 0
    url: str
    createdDate: datetime | None = None
    lastUpdated: datetime | None = None
    downloadCount: int = 0
    voteCount: int = 0
    viewCount: int = 0
    licenseName: str | None = None
    description: str | None = None
    data: list[dict] | None = Field(default_factory=list, description="Dataset files info")

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @property
    def external_id(self) -> str:
        """Returns dataset reference."""
        return self.ref

    @property
    def column_names(self) -> list[str]:
        """Extract column names from dataset files metadata."""
        columns = []
        if self.data:
            for file in self.data:
                if isinstance(file, dict) and "columns" in file:
                    columns.extend(file["columns"])
        return columns

    def get_update_time(self) -> datetime:
        """Get dataset update time safely."""
        if self.lastUpdated:
            return self.lastUpdated
        if self.createdDate:
            return self.createdDate
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
    description: str | None = None
    url: HttpUrl
    score: float = Field(..., description="Relevance score (0.0 - 1.0)")
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class SearchResponse(BaseModel):
    items: list[DatasetItem]
    total: int
    execution_time_ms: float
