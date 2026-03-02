from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AssetInput(StrictBaseModel):
    asset_id: str
    lat: float
    lon: float
    name: str | None = None


class BatchScoresRequest(StrictBaseModel):
    assets: list[AssetInput]
    start_date: date
    end_date: date
    climatology_version: str
    include_perils: list[Literal["heat", "rain", "wind", "drought", "wildfire", "all"]]

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class ScoreSeriesPoint(StrictBaseModel):
    date: date
    scores: dict[str, float]
    bands: dict[str, str]
    drivers: dict[str, list[str]] | None = None


class BatchScoresResult(StrictBaseModel):
    asset_id: str
    series: list[ScoreSeriesPoint]


class BatchScoresResponse(StrictBaseModel):
    run_id: str
    climatology_version: str
    results: list[BatchScoresResult]

