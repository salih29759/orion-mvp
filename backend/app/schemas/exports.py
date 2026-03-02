from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExportPortfolioRequest(StrictBaseModel):
    portfolio_id: str
    start_date: date
    end_date: date
    format: Literal["csv"]
    include_drivers: bool = True

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class ExportPortfolioResponse(StrictBaseModel):
    export_id: str
    status: Literal["queued", "running", "success", "failed"]
    path: str
    download_url: str | None = None

