from datetime import datetime, date

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ProvinceORM(Base):
    __tablename__ = "provinces"

    id: Mapped[str] = mapped_column(String(4), primary_key=True)  # plate number as string
    plate: Mapped[int] = mapped_column(Integer, unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    region: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    lng: Mapped[float] = mapped_column(Float, nullable=False)
    population: Mapped[int] = mapped_column(Integer, nullable=False)
    insured_assets: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class DailyScoreORM(Base):
    __tablename__ = "daily_scores"
    __table_args__ = (UniqueConstraint("province_id", "as_of_date", name="uq_daily_scores_province_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    province_id: Mapped[str] = mapped_column(String(4), ForeignKey("provinces.id", ondelete="CASCADE"), nullable=False, index=True)
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    flood_score: Mapped[int] = mapped_column(Integer, nullable=False)
    drought_score: Mapped[int] = mapped_column(Integer, nullable=False)
    overall_score: Mapped[int] = mapped_column(Integer, nullable=False)
    risk_level: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    trend: Mapped[str] = mapped_column(String(16), nullable=False)
    trend_pct: Mapped[float] = mapped_column(Float, nullable=False)
    rain_7d_mm: Mapped[float] = mapped_column(Float, nullable=False)
    rain_60d_mm: Mapped[float] = mapped_column(Float, nullable=False)
    data_source: Mapped[str] = mapped_column(String(64), nullable=False)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class AlertORM(Base):
    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    province_id: Mapped[str] = mapped_column(String(4), ForeignKey("provinces.id", ondelete="CASCADE"), nullable=False, index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    risk_type: Mapped[str] = mapped_column(String(16), nullable=False)
    affected_policies: Mapped[int] = mapped_column(Integer, nullable=False)
    estimated_loss_usd: Mapped[float] = mapped_column(Float, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true", index=True)


class PipelineRunORM(Base):
    __tablename__ = "pipeline_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    rows_written: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class Era5IngestJobORM(Base):
    __tablename__ = "era5_ingest_jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_signature: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    dataset: Mapped[str] = mapped_column(String(128), nullable=False)
    variables_csv: Mapped[str] = mapped_column(Text, nullable=False)
    bbox_csv: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default="cds")
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="bbox")
    points_set: Mapped[str | None] = mapped_column(String(64), nullable=True)
    month_label: Mapped[str | None] = mapped_column(String(7), nullable=True, index=True)
    source_range_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    rows_written: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bytes_downloaded: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    raw_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    feature_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dq_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    dq_report_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Era5ArtifactORM(Base):
    __tablename__ = "era5_artifacts"
    __table_args__ = (UniqueConstraint("request_signature", "artifact_type", "gcs_uri", name="uq_era5_artifact_sig_type_uri"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_signature: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    job_id: Mapped[str] = mapped_column(String(64), ForeignKey("era5_ingest_jobs.job_id", ondelete="CASCADE"), nullable=False, index=True)
    artifact_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)  # raw_nc | feature_daily_parquet
    dataset: Mapped[str] = mapped_column(String(128), nullable=False)
    variables_csv: Mapped[str] = mapped_column(Text, nullable=False)
    bbox_csv: Mapped[str] = mapped_column(String(128), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    gcs_uri: Mapped[str] = mapped_column(Text, nullable=False)
    source_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_etag: Mapped[str | None] = mapped_column(String(255), nullable=True)
    cache_hit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    byte_size: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Era5BackfillJobORM(Base):
    __tablename__ = "era5_backfill_jobs"

    backfill_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_signature: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True, default="running")
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="monthly")
    dataset: Mapped[str] = mapped_column(String(128), nullable=False)
    variables_csv: Mapped[str] = mapped_column(Text, nullable=False)
    bbox_csv: Mapped[str] = mapped_column(String(128), nullable=False)
    start_month: Mapped[str] = mapped_column(String(7), nullable=False)
    end_month: Mapped[str] = mapped_column(String(7), nullable=False)
    months_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    months_success: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    months_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provider_strategy: Mapped[str] = mapped_column(String(32), nullable=False, default="aws_first_hybrid")
    force: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    failed_months_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Era5BackfillItemORM(Base):
    __tablename__ = "era5_backfill_items"
    __table_args__ = (UniqueConstraint("backfill_id", "month_label", name="uq_backfill_month"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    backfill_id: Mapped[str] = mapped_column(String(64), ForeignKey("era5_backfill_jobs.backfill_id", ondelete="CASCADE"), nullable=False, index=True)
    month_label: Mapped[str] = mapped_column(String(7), nullable=False, index=True)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    job_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("era5_ingest_jobs.job_id", ondelete="SET NULL"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued", index=True)
    provider_selected: Mapped[str] = mapped_column(String(16), nullable=False, default="cds")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AwsEra5ObjectORM(Base):
    __tablename__ = "aws_era5_objects"
    __table_args__ = (UniqueConstraint("bucket", "key", name="uq_aws_era5_bucket_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bucket: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    etag: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    last_modified: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    dataset_group: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    variable: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    month: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    day: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class AwsEra5CatalogRunORM(Base):
    __tablename__ = "aws_era5_catalog_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    objects_scanned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class BackfillProgressORM(Base):
    __tablename__ = "backfill_progress"

    month: Mapped[date] = mapped_column(Date, primary_key=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_msg: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)


class SentinelBackfillProgressORM(Base):
    __tablename__ = "sentinel_backfill_progress"
    __table_args__ = (UniqueConstraint("run_id", "year", "month", name="uq_sentinel_backfill_run_month"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    month: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued", index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rows_written: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_msg: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class ExportJobORM(Base):
    __tablename__ = "export_jobs"

    export_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    scenario: Mapped[str] = mapped_column(String(32), nullable=False, default="historical")
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    output_format: Mapped[str] = mapped_column(String(16), nullable=False, default="csv")
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True, default="success")
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    gcs_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    signed_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ClimatologyRunORM(Base):
    __tablename__ = "climatology_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    climatology_version: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    dataset: Mapped[str] = mapped_column(String(128), nullable=False, default="era5-land")
    baseline_start: Mapped[date] = mapped_column(Date, nullable=False)
    baseline_end: Mapped[date] = mapped_column(Date, nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="month")
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True, default="success")
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    thresholds_gcs_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ClimatologyThresholdORM(Base):
    __tablename__ = "climatology_thresholds"
    __table_args__ = (
        UniqueConstraint("climatology_version", "cell_lat", "cell_lng", "month", name="uq_clim_version_cell_month"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    climatology_version: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    cell_lat: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    cell_lng: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    month: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    temp_max_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    wind_max_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_1d_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_1d_p99: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_7d_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_7d_p99: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_30d_p10: Mapped[float | None] = mapped_column(Float, nullable=True)
    soil_moisture_p10: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ClimatologyThresholdDoyORM(Base):
    __tablename__ = "climatology_thresholds_doy"
    __table_args__ = (
        UniqueConstraint("climatology_version", "cell_lat", "cell_lng", "doy", name="uq_clim_version_cell_doy"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    climatology_version: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    cell_lat: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    cell_lng: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    doy: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    temp_max_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    wind_max_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_1d_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_1d_p99: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_7d_p95: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_7d_p99: Mapped[float | None] = mapped_column(Float, nullable=True)
    precip_30d_p10: Mapped[float | None] = mapped_column(Float, nullable=True)
    soil_moisture_p10: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class AssetRiskScoreORM(Base):
    __tablename__ = "asset_risk_scores"
    __table_args__ = (
        UniqueConstraint(
            "asset_id",
            "score_date",
            "peril",
            "scenario",
            "horizon",
            "likelihood",
            name="uq_asset_risk_score_dim",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    score_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    peril: Mapped[str] = mapped_column(String(32), nullable=False, index=True)  # heat|rain|wind|drought
    scenario: Mapped[str] = mapped_column(String(32), nullable=False, default="historical", index=True)
    horizon: Mapped[str] = mapped_column(String(32), nullable=False, default="current")
    likelihood: Mapped[str] = mapped_column(String(32), nullable=False, default="observed")
    score_0_100: Mapped[int] = mapped_column(Integer, nullable=False)
    band: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    exposure_json: Mapped[str] = mapped_column(Text, nullable=False)
    drivers_json: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    climatology_version: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    data_version: Mapped[str] = mapped_column(String(128), nullable=False, default="era5_daily_v1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class PortfolioAssetORM(Base):
    __tablename__ = "portfolio_assets"
    __table_args__ = (UniqueConstraint("portfolio_id", "asset_id", name="uq_portfolio_asset"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    asset_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    lon: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class FirmsIngestJobORM(Base):
    __tablename__ = "firms_ingest_jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_signature: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True, default="queued")
    source: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    bbox_csv: Mapped[str] = mapped_column(String(128), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    rows_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    raw_gcs_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class FireEventORM(Base):
    __tablename__ = "fires"
    __table_args__ = (
        UniqueConstraint("source", "time_utc", "lat_round", "lon_round", name="uq_fire_event_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    time_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    lat: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    lon: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    lat_round: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    lon_round: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    geom_wkt: Mapped[str | None] = mapped_column(Text, nullable=True)
    frp: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[str | None] = mapped_column(String(32), nullable=True)
    satellite: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    raw_job_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("firms_ingest_jobs.job_id", ondelete="SET NULL"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class NotificationORM(Base):
    __tablename__ = "notifications"
    __table_args__ = (UniqueConstraint("dedup_key", name="uq_notification_dedup"),)

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    customer_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    portfolio_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    asset_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    dedup_key: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
