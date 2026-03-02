from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Portfolio(StrictBaseModel):
    portfolio_id: str
    name: str


class BandCounts(StrictBaseModel):
    minimal: int = 0
    minor: int = 0
    moderate: int = 0
    major: int = 0
    extreme: int = 0


class RiskTrendPoint(StrictBaseModel):
    date: date
    scores: dict[str, float]


class TopAsset(StrictBaseModel):
    asset_id: str
    name: str
    lat: float
    lon: float
    band: str
    scores: dict[str, float]


class PeriodRange(StrictBaseModel):
    start: date
    end: date


class RiskSummaryResponse(StrictBaseModel):
    portfolio_id: str
    period: PeriodRange
    bands: BandCounts
    peril_averages: dict[str, float]
    top_assets: list[TopAsset]
    trend: list[RiskTrendPoint]

