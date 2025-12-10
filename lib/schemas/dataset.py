from uuid import UUID
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl, ConfigDict


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
