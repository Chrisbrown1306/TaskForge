from typing import Any

from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    type: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9_.-]+$", examples=["echo"])
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=10)
    delay_seconds: float = Field(default=0, ge=0, le=86_400)


class JobAccepted(BaseModel):
    id: str
    status: str
    duplicate: bool = False


class JobOut(BaseModel):
    id: str
    type: str
    status: str
    attempts: int
    max_attempts: int
    payload: dict[str, Any] | None = None
    result: Any | None = None
    error: str | None = None
    created_at: float
    updated_at: float
    started_at: float | None = None
    finished_at: float | None = None
