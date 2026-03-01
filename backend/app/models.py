from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from app.config import settings


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class RiskLevel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class RiskType(str, Enum):
    FLOOD = "FLOOD"
    DROUGHT = "DROUGHT"


class Trend(str, Enum):
    UP = "UP"
    DOWN = "DOWN"
    STABLE = "STABLE"


class AlertLevel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"


# ---------------------------------------------------------------------------
# Domain models (data shapes stored in / read from Firestore)
# ---------------------------------------------------------------------------


class Province(BaseModel):
    id: str
    plate: int
    name: str
    region: str
    lat: float
    lng: float
    population: int
    insured_assets: int = Field(description="Estimated insured asset value in USD")
    flood_score: int = Field(ge=0, le=100)
    drought_score: int = Field(ge=0, le=100)
    overall_score: int = Field(ge=0, le=100)
    risk_level: RiskLevel
    trend: Trend
    trend_pct: float
    rain_7d_mm: float | None = None
    rain_60d_mm: float | None = None
    data_source: str | None = None
    as_of_date: date | None = None


class Alert(BaseModel):
    id: str
    province_id: str
    province_name: str
    level: AlertLevel
    risk_type: RiskType
    affected_policies: int
    estimated_loss_usd: float = Field(description="Estimated loss in USD")
    estimated_loss: float = Field(description="Deprecated alias of estimated_loss_usd")
    message: str
    issued_at: datetime


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class PortfolioAnalyzeRequest(BaseModel):
    province_id: str = Field(examples=["34"])
    policy_count: int = Field(gt=0, examples=[1500])
    sum_insured: float = Field(gt=0, description="Total portfolio sum insured in USD", examples=[250_000_000])


# ---------------------------------------------------------------------------
# Response envelope
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


class BaseResponse(BaseModel):
    status: str = "success"
    generated_at: str = Field(default_factory=_now_iso)
    model_version: str = settings.model_version
    confidence_score: float = settings.confidence_score
    data_source: str | None = None
    as_of_date: date | None = None


class ProvincesResponse(BaseResponse):
    data: list[Province]
    pagination: dict[str, Any]


class ProvinceDetailResponse(BaseResponse):
    data: Province


class AlertsResponse(BaseResponse):
    data: list[Alert]
    pagination: dict[str, Any]


# ---------------------------------------------------------------------------
# Portfolio analysis response models
# ---------------------------------------------------------------------------


class HazardExposure(BaseModel):
    score: int
    expected_loss_usd: float
    loss_ratio: float


class PortfolioAnalysis(BaseModel):
    province_id: str
    province_name: str
    risk_level: RiskLevel
    policy_count: int
    sum_insured: float
    flood: HazardExposure
    drought: HazardExposure
    total_expected_loss_usd: float
    combined_loss_ratio: float
    suggested_annual_premium_usd: float
    recommendations: list[str]


class PortfolioAnalyzeResponse(BaseResponse):
    data: PortfolioAnalysis
