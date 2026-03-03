from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SentinelBackfillRequest(StrictBaseModel):
    start: date
    end: date
    concurrency: int = Field(default=2, ge=1, le=4)
    force: bool = False

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class SentinelBackfillAcceptedResponse(StrictBaseModel):
    run_id: str
    total_months: int


class SentinelBackfillStatusResponse(StrictBaseModel):
    run_id: str | None = None
    total_months: int
    completed: int
    failed: int
    running: int
    percent_done: float
    last_updated: str | None = None
    failed_months: list[str]
