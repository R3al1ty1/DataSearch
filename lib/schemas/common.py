from pydantic import BaseModel, Field
from lib.core.constants import AppEnvironment


class HealthResponse(BaseModel):
    status: str = Field(..., description="Service status", example="active")
    environment: AppEnvironment
    version: str = "1.0.0"


class ErrorResponse(BaseModel):
    detail: str = Field(..., description="Error description")
