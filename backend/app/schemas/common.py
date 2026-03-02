from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ErrorResponse(StrictBaseModel):
    error_code: str
    message: str
    details: dict[str, Any] | None = None


class HealthResponse(StrictBaseModel):
    status: str
    version: str | None = None


class MetricsResponse(StrictBaseModel):
    jobs_last_24h: int
    success_rate: float
    avg_duration_seconds: float
    bytes_downloaded_last_24h: int


class BBox(StrictBaseModel):
    north: float
    west: float
    south: float
    east: float


class JobStatusResponse(StrictBaseModel):
    job_id: str
    status: Literal["queued", "running", "success", "failed", "success_with_warnings", "fail_dq"]
    type: str
    created_at: datetime
    updated_at: datetime | None = None
    progress: dict[str, Any] | None = None
    children: list[str] = Field(default_factory=list)


class BackfillRequest(StrictBaseModel):
    start_month: str
    end_month: str
    bbox: BBox
    variables: list[str]
    mode: Literal["monthly"]
    concurrency: int = Field(default=2, ge=1, le=10)

    @model_validator(mode="after")
    def validate_months(self):
        if len(self.start_month) != 7 or len(self.end_month) != 7:
            raise ValueError("start_month/end_month must be YYYY-MM")
        if self.start_month > self.end_month:
            raise ValueError("start_month must be <= end_month")
        return self


class ClimatologyBuildRequest(StrictBaseModel):
    version: str
    baseline_start: date
    baseline_end: date
    level: Literal["month", "doy"]

    @model_validator(mode="after")
    def validate_dates(self):
        if self.baseline_start > self.baseline_end:
            raise ValueError("baseline_start must be <= baseline_end")
        return self


class ClimatologyBuildResponse(StrictBaseModel):
    version: str
    status: Literal["success", "failed", "running"]
    row_count: int


class AssetWildfireFeaturesResponse(StrictBaseModel):
    status: str
    asset_id: str
    window: Literal["24h", "7d"]
    nearest_fire_distance_km: float | None = None
    fires_within_10km_count: int
    max_frp_within_20km: float | None = None

