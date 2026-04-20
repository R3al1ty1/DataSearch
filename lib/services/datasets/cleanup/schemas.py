from pydantic import BaseModel, Field


class CleanupTriggerRequest(BaseModel):
    batch_size: int = Field(default=200, ge=1, le=10000)
    stale_after_hours: int = Field(default=48, ge=1)


class CleanupTriggerResponse(BaseModel):
    task_id: str
    status: str
    message: str
