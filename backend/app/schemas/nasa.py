from __future__ import annotations

from datetime import date

from pydantic import model_validator

from app.schemas.common import JobStatusResponse, StrictBaseModel


class NasaBackfillRequest(StrictBaseModel):
    start: date
    end: date

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start > self.end:
            raise ValueError("start must be <= end")
        return self


class NasaStatusResponse(StrictBaseModel):
    smap: JobStatusResponse | None = None
    modis: JobStatusResponse | None = None
