from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


SourceType = Literal["aws_era5", "firms", "nasa_smap", "nasa_modis"]
JobType = Literal["backfill", "daily", "monthly"]
ChunkingType = Literal["monthly", "daily", "range"]


class EnqueueRequest(StrictBaseModel):
    source: SourceType
    job_type: JobType
    start: date
    end: date
    chunking: ChunkingType = "monthly"
    concurrency: int = Field(default=1, ge=1, le=128)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class EnqueueResponse(StrictBaseModel):
    status: Literal["accepted"]
    source: SourceType
    job_type: JobType
    run_id: str
    published_count: int
    deduped_count: int
    skipped_count: int
    disabled: bool = False
    reason: str | None = None


class PubSubJobMessage(StrictBaseModel):
    source: SourceType
    job_type: JobType
    chunk: dict[str, Any]
    run_id: str
    attempt: int = Field(default=1, ge=1)
    chunk_id: str
    idempotency_key: str
    concurrency: int | None = Field(default=None, ge=1, le=128)


class PubSubPushMessage(StrictBaseModel):
    data: str
    message_id: str | None = Field(default=None, alias="messageId")
    publish_time: str | None = Field(default=None, alias="publishTime")
    attributes: dict[str, str] = Field(default_factory=dict)


class PubSubPushEnvelope(StrictBaseModel):
    message: PubSubPushMessage
    subscription: str | None = None
    delivery_attempt: int | None = Field(default=None, alias="deliveryAttempt")

    def decode_job_message(self) -> PubSubJobMessage:
        import base64
        import json

        raw = base64.b64decode(self.message.data).decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("message data is not valid JSON") from exc
        return PubSubJobMessage.model_validate(payload)
