from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    topic: str = Field(min_length=1, max_length=120)
    event_id: str = Field(min_length=1, max_length=180)
    timestamp: datetime
    source: str = Field(min_length=1, max_length=120)
    payload: dict[str, Any]

    @field_validator("topic", "event_id", "source")
    @classmethod
    def must_not_be_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    def to_record(self) -> dict[str, Any]:
        timestamp = self.timestamp
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return {
            "topic": self.topic,
            "event_id": self.event_id,
            "timestamp": timestamp.isoformat(),
            "source": self.source,
            "payload": self.payload,
        }


class PublishResponse(BaseModel):
    accepted: int
    queued: int
    message: str
