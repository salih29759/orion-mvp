from __future__ import annotations

from datetime import date, datetime

from pydantic import Field, model_validator

from app.schemas.common import StrictBaseModel


class NoaaBackfillRequest(StrictBaseModel):
    start: date
    end: date
    concurrency: int = Field(default=2, ge=1, le=8)
    force: bool = False

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class NoaaBackfillAcceptedResponse(StrictBaseModel):
    run_id: str
    status: str
    type: str
    created_at: datetime
    total_months: int
    effective_start: date
    effective_end: date
    progress: dict


class NoaaBackfillStatusResponse(StrictBaseModel):
    run_id: str | None = None
    total_months: int
    completed: int
    failed: int
    running: int
    pending: int
    percent_done: float
    rows_written: int
    stations_total: int
    stations_success: int
    stations_failed: int
    strong_wind_proxy_used: int
    last_updated: str | None = None
    recent_errors: list[dict]
    status: str
