from __future__ import annotations

from datetime import date

from pydantic import Field, model_validator

from app.schemas.common import StrictBaseModel


class EarthquakeBackfillRequest(StrictBaseModel):
    start: date
    end: date
    min_magnitude: float = Field(default=2.5, ge=0.0, le=10.0)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self
