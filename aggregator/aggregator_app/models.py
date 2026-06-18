from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1, max_length=120)
    event_id: str = Field(min_length=1, max_length=200)
    timestamp: datetime
    source: str = Field(min_length=1, max_length=120)
    payload: dict[str, Any] = Field(default_factory=dict)


class EventOut(EventIn):
    processed_at: datetime


class PublishResult(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int


class Stats(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime_seconds: float


class DemoResponse(BaseModel):
    use_case: str
    explanation: str
    result: PublishResult
    stats: Stats