from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import Field, model_validator

from app.schemas.common import StrictBaseModel


class OpenaqBackfillRequest(StrictBaseModel):
    start: date
    end: date
    concurrency: int = Field(default=5, ge=1, le=10)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class OpenaqStatusResponse(StrictBaseModel):
    run_id: str | None = None
    status: Literal["idle", "queued", "running", "completed", "completed_with_failures", "failed"]
    total_months: int = 0
    completed: int = 0
    failed: int = 0
    running: int = 0
    pending: int = 0
    rows_written: int = 0
    requested_start: date | None = None
    requested_end: date | None = None
    effective_end: date | None = None
    stations_total: int = 0
    stations_processed: int = 0
    metadata_gcs_uri: str | None = None
    last_updated: datetime | None = None
    recent_errors: list[dict[str, Any]] = Field(default_factory=list)
    warnings: dict[str, int] = Field(default_factory=dict)
