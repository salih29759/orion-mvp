from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, Field, model_validator

from app.config import settings
from app.era5_presets import variables_for_profile


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
    WILDFIRE = "WILDFIRE"


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


class Era5IngestRequest(BaseModel):
    dataset: str = "era5-land"
    variable_profile: Literal["core", "full"] = "core"
    variables: list[str] | None = None
    start_date: date
    end_date: date
    bbox: dict[str, float] = Field(
        default_factory=lambda: {"north": 42.0, "west": 26.0, "south": 36.0, "east": 45.0}
    )
    format: str = "netcdf"

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        required_bbox = {"north", "west", "south", "east"}
        if set(self.bbox.keys()) != required_bbox:
            raise ValueError("bbox must contain exactly: north, west, south, east")
        if self.dataset not in {"era5-land", "reanalysis-era5-land"}:
            raise ValueError("dataset must be 'era5-land' or 'reanalysis-era5-land'")
        if self.format not in {"netcdf"}:
            raise ValueError("format must be 'netcdf'")
        if self.variables and len(self.variables) == 0:
            raise ValueError("variables cannot be empty")
        if not self.variables:
            self.variables = variables_for_profile(self.variable_profile)
        return self


class Era5IngestResponse(BaseModel):
    status: str
    job_id: str
    deduplicated: bool
    request_signature: str


class JobStatusResponse(BaseModel):
    status: str
    job_id: str
    request_signature: str
    dataset: str
    variables: list[str]
    bbox: list[float]
    start_date: date
    end_date: date
    rows_written: int
    bytes_downloaded: int
    raw_files: int
    feature_files: int
    dq_status: str | None = None
    dq_report: list[dict[str, Any]] | None = None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None


class ClimatePoint(BaseModel):
    date: date
    temp_mean: float | None = None
    temp_max: float | None = None
    precip_sum: float | None = None
    wind_max: float | None = None
    soil_moisture_mean: float | None = None
    source: str


class ClimateSeriesResponse(BaseResponse):
    data: list[ClimatePoint]


class Era5BackfillRequest(BaseModel):
    start_month: str = Field(examples=["2018-01"])
    end_month: str = Field(examples=["2024-12"])
    bbox: dict[str, float] = Field(
        default_factory=lambda: {"north": 42.0, "west": 26.0, "south": 36.0, "east": 45.0}
    )
    variable_profile: Literal["core", "full"] = "core"
    variables: list[str] | None = None
    mode: str = "monthly"
    dataset: str = "era5-land"
    concurrency: int = Field(default=2, ge=1, le=5)

    @model_validator(mode="after")
    def validate_months(self):
        if len(self.start_month) != 7 or len(self.end_month) != 7:
            raise ValueError("start_month/end_month must be YYYY-MM")
        if self.start_month > self.end_month:
            raise ValueError("start_month must be <= end_month")
        if self.mode != "monthly":
            raise ValueError("mode must be 'monthly'")
        if self.variables and len(self.variables) == 0:
            raise ValueError("variables cannot be empty")
        if not self.variables:
            self.variables = variables_for_profile(self.variable_profile)
        return self


class Era5BackfillResponse(BaseModel):
    status: str
    backfill_id: str
    deduplicated: bool
    months_total: int


class Era5BackfillStatusResponse(BaseModel):
    status: str
    backfill_id: str
    start_month: str
    end_month: str
    months_total: int
    months_success: int
    months_failed: int
    failed_months: list[str]
    child_jobs: list[dict[str, Any]] | None = None
    created_at: datetime
    finished_at: datetime | None


class AssetPoint(BaseModel):
    asset_id: str = Field(
        validation_alias=AliasChoices("asset_id", "id"),
        serialization_alias="asset_id",
    )
    lat: float
    lon: float
    name: str | None = None

    @property
    def id(self) -> str:
        # Backward-compatible accessor used by existing code.
        return self.asset_id


class Era5BatchFeatureRequest(BaseModel):
    assets: list[AssetPoint]
    start_date: date
    end_date: date


class PortfolioExportRequest(BaseModel):
    portfolio_id: str
    scenario: str = "historical"
    start_date: date
    end_date: date
    format: str = "csv"
    include_drivers: bool = True
    climatology_version: str = "v1_baseline_2015_2024"
    include_wildfire: bool = False
    assets: list[AssetPoint] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class PortfolioExportResponse(BaseModel):
    export_id: str
    status: Literal["queued", "running", "success", "failed"]
    path: str
    download_url: str | None = None


class ClimatologyBuildRequest(BaseModel):
    climatology_version: str = Field(
        default="v1_baseline_2015_2024",
        validation_alias=AliasChoices("version", "climatology_version"),
        serialization_alias="version",
    )
    baseline_start: date
    baseline_end: date
    level: Literal["month", "doy"] = "month"

    @model_validator(mode="after")
    def validate_dates(self):
        if self.baseline_start > self.baseline_end:
            raise ValueError("baseline_start must be <= baseline_end")
        return self


class ClimatologyBuildResponse(BaseModel):
    version: str
    status: Literal["success", "failed", "running"]
    row_count: int


class ScoreBatchRequest(BaseModel):
    assets: list[AssetPoint]
    start_date: date
    end_date: date
    climatology_version: str = "v1_baseline_2015_2024"
    include_perils: list[Literal["heat", "rain", "wind", "drought", "wildfire", "all"]] = Field(
        default_factory=lambda: ["heat", "rain", "wind", "drought"]
    )
    persist: bool = True

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class ScorePoint(BaseModel):
    date: date
    peril: str
    score_0_100: int
    band: str
    exposure: dict[str, Any]
    drivers: list[str]


class ScoreSeriesPoint(BaseModel):
    date: date
    scores: dict[str, float]
    bands: dict[str, str]
    drivers: dict[str, list[str]] | None = None


class BatchScoresResult(BaseModel):
    asset_id: str
    series: list[ScoreSeriesPoint]


class ScoreBatchResponse(BaseModel):
    run_id: str
    climatology_version: str
    results: list[BatchScoresResult]


class ScoreBenchmarkRequest(BaseModel):
    assets_count: int = Field(default=100, ge=1, le=5000)
    start_date: date
    end_date: date
    climatology_version: str = "v1_baseline_2015_2024"


class ScoreBenchmarkResponse(BaseModel):
    status: str
    run_id: str
    assets_count: int
    days: int
    duration_seconds: float
    per_asset_ms: float
    per_asset_day_ms: float


class RiskTrendPoint(BaseModel):
    date: date
    scores: dict[str, float]


class TopAssetItem(BaseModel):
    asset_id: str
    name: str
    lat: float
    lon: float
    band: str
    scores: dict[str, float]


class BandCounts(BaseModel):
    minimal: int = 0
    minor: int = 0
    moderate: int = 0
    major: int = 0
    extreme: int = 0


class PeriodRange(BaseModel):
    start: date
    end: date


class PortfolioItem(BaseModel):
    portfolio_id: str
    name: str


class PortfolioRiskSummaryResponse(BaseModel):
    portfolio_id: str
    period: PeriodRange
    bands: BandCounts
    peril_averages: dict[str, float]
    top_assets: list[TopAssetItem]
    trend: list[RiskTrendPoint]


class FirmsIngestRequest(BaseModel):
    source: str = "VIIRS_SNPP_NRT"
    bbox: dict[str, float] = Field(
        default_factory=lambda: {"north": 42.0, "west": 26.0, "south": 36.0, "east": 45.0}
    )
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        required_bbox = {"north", "west", "south", "east"}
        if set(self.bbox.keys()) != required_bbox:
            raise ValueError("bbox must contain exactly: north, west, south, east")
        return self


class FirmsIngestResponse(BaseModel):
    status: str
    request_status: str | None = None
    job_id: str
    type: str = "firms_ingest"
    created_at: datetime
    deduplicated: bool


class WildfireFeaturesResponse(BaseModel):
    status: str
    asset_id: str
    window: str
    nearest_fire_distance_km: float | None
    fires_within_10km_count: int
    max_frp_within_20km: float | None = None


class NotificationItem(BaseModel):
    id: str
    severity: Literal["low", "medium", "high"]
    type: str
    portfolio_id: str | None = None
    asset_id: str
    created_at: datetime
    acknowledged_at: datetime | None = None
    payload: dict[str, Any]


class AckNotificationResponse(BaseModel):
    id: str
    acknowledged_at: datetime
