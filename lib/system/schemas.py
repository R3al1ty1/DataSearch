from pydantic import BaseModel, Field
from lib.core.constants import AppEnvironment
from lib.core.error_codes import ErrorCode

class HealthResponse(BaseModel):
    status: str = Field(..., description="Service status", example="active")
    environment: AppEnvironment
    version: str = "1.0.0"

class ErrorResponse(BaseModel):
    error_code: ErrorCode = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    details: dict | None = Field(default=None, description="Error details")
