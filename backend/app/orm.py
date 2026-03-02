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
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


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
