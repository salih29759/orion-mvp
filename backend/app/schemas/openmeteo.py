from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OpenMeteoBackfillRequest(StrictBaseModel):
    start: date
    end: date
    concurrency: int = Field(default=10, ge=1, le=10)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class OpenMeteoForecastRequest(StrictBaseModel):
    forecast_days: int = Field(default=16, ge=1, le=16)
