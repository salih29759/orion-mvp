from __future__ import annotations

from datetime import date

from pydantic import Field, model_validator

from app.schemas.common import StrictBaseModel


class GlofasBackfillRequest(StrictBaseModel):
    start: date
    end: date
    concurrency: int = Field(default=2, ge=1, le=4)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class GlofasStatusResponse(StrictBaseModel):
    run_id: str
    status: str
    total_months: int
    completed: int
    failed: int
    running: int
    percent_done: float
    failed_months: list[str]
    eta_hours: float | None = None
    last_updated: str
    baseline_ready: bool
    baseline_uri: str | None = None
