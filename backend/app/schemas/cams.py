from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CamsBackfillRequest(StrictBaseModel):
    start: date
    end: date
    concurrency: int = Field(default=1, ge=1, le=2)
    force: bool = False

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class CamsBackfillAcceptedResponse(StrictBaseModel):
    run_id: str
    status: Literal["queued", "running", "failed", "success"]
    type: str
    created_at: datetime
    progress: dict[str, Any]


class CamsBackfillStatusResponse(StrictBaseModel):
    run_id: str | None
    total_months: int
    completed: int
    failed: int
    running: int
    pending: int
    percent_done: float
    last_updated: str
    recent_errors: list[str]
    status: str
