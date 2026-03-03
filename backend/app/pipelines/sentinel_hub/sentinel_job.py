from __future__ import annotations

from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import logging
import threading
import time
from uuid import uuid4

import numpy as np
import pandas as pd
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.errors import ApiError
from app.orm import SentinelBackfillProgressORM
from app.pipelines.sentinel_hub.sentinel_client import fetch_monthly_raster, request_access_token, validate_sentinel_runtime
from app.pipelines.sentinel_hub.sentinel_sample import centroid_bbox, load_province_centroids
from app.pipelines.sentinel_hub.sentinel_writer import month_exists, write_month_parquet

LOG = logging.getLogger("orion.sentinel.job")

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"

MIN_START_DATE = date(2017, 1, 1)


def month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def month_end(value: date) -> date:
    return date(value.year, value.month, monthrange(value.year, value.month)[1])


def iter_month_starts(start_value: date, end_value: date) -> list[date]:
    cur = month_start(start_value)
    end_month = month_start(end_value)
    out: list[date] = []
    while cur <= end_month:
        out.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def last_completed_month_end(today: date) -> date:
    first_day_current_month = date(today.year, today.month, 1)
    return first_day_current_month - timedelta(days=1)


def normalize_range(start_value: date, end_value: date) -> tuple[date, date]:
    if start_value > end_value:
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message="start must be <= end")

    normalized_start = max(start_value, MIN_START_DATE)
    capped_end = min(end_value, last_completed_month_end(datetime.now(timezone.utc).date()))
    if capped_end < normalized_start:
        raise ApiError(
            status_code=422,
            error_code="VALIDATION_ERROR",
            message="Requested range has no completed Sentinel-2 L2A months available",
            details={
                "min_start": MIN_START_DATE.isoformat(),
                "last_completed_day": last_completed_month_end(datetime.now(timezone.utc).date()).isoformat(),
            },
        )
    return normalized_start, capped_end


def aggregate_metrics(raster: np.ndarray) -> dict[str, float | None]:
    arr = np.asarray(raster, dtype=np.float64)
    if arr.ndim != 3:
        raise RuntimeError(f"Expected 3D raster, got shape={arr.shape}")

    if arr.shape[-1] == 4:
        cube = arr
    elif arr.shape[0] == 4:
        cube = np.transpose(arr, (1, 2, 0))
    else:
        raise RuntimeError(f"Expected 4 bands, got shape={arr.shape}")

    ndvi = cube[..., 0]
    nbr = cube[..., 1]
    bai = cube[..., 2]
    data_mask = cube[..., 3]

    total_pixels = int(data_mask.size)
    if total_pixels == 0:
        return {
            "ndvi_mean": None,
            "ndvi_min": None,
            "nbr_mean": None,
            "bai_max": None,
            "cloud_coverage_pct": 100.0,
        }

    valid_pixels = np.isfinite(data_mask) & (data_mask > 0)
    valid_count = int(np.count_nonzero(valid_pixels))
    valid_ratio = float(valid_count / total_pixels)
    cloud_coverage_pct = float(100.0 * (1.0 - valid_ratio))

    if valid_count == 0:
        return {
            "ndvi_mean": None,
            "ndvi_min": None,
            "nbr_mean": None,
            "bai_max": None,
            "cloud_coverage_pct": cloud_coverage_pct,
        }

    ndvi_values = np.clip(ndvi[valid_pixels & np.isfinite(ndvi)], -1.0, 1.0)
    nbr_values = nbr[valid_pixels & np.isfinite(nbr)]
    bai_values = bai[valid_pixels & np.isfinite(bai)]

    return {
        "ndvi_mean": float(np.nanmean(ndvi_values)) if ndvi_values.size else None,
        "ndvi_min": float(np.nanmin(ndvi_values)) if ndvi_values.size else None,
        "nbr_mean": float(np.nanmean(nbr_values)) if nbr_values.size else None,
        "bai_max": float(np.nanmax(bai_values)) if bai_values.size else None,
        "cloud_coverage_pct": cloud_coverage_pct,
    }


def build_month_rows(
    *,
    month_value: date,
    run_id: str,
    raster_fetcher=fetch_monthly_raster,
) -> pd.DataFrame:
    provinces = load_province_centroids()
    half_size = float(settings.sentinel_bbox_half_size_deg)
    end_value = month_end(month_value)
    now = datetime.now(timezone.utc)
    rows: list[dict] = []

    for province in provinces.itertuples(index=False):
        bbox = centroid_bbox(lat=float(province.lat), lon=float(province.lon), half_size_deg=half_size)
        raster = raster_fetcher(
            bbox=bbox,
            month_start=month_value,
            month_end=end_value,
        )
        metrics = aggregate_metrics(raster)
        rows.append(
            {
                "year": int(month_value.year),
                "month": int(month_value.month),
                "province_id": str(province.province_id),
                "lat": float(province.lat),
                "lon": float(province.lon),
                "ndvi_mean": metrics["ndvi_mean"],
                "ndvi_min": metrics["ndvi_min"],
                "nbr_mean": metrics["nbr_mean"],
                "bai_max": metrics["bai_max"],
                "cloud_coverage_pct": metrics["cloud_coverage_pct"],
                "source": "sentinel2_l2a",
                "run_id": run_id,
                "ingested_at": now,
            }
        )

    frame = pd.DataFrame(rows)
    columns = [
        "year",
        "month",
        "province_id",
        "lat",
        "lon",
        "ndvi_mean",
        "ndvi_min",
        "nbr_mean",
        "bai_max",
        "cloud_coverage_pct",
        "source",
        "run_id",
        "ingested_at",
    ]
    return frame[columns].sort_values(["province_id"]).reset_index(drop=True)


def _insert_pending_rows(*, run_id: str, months: list[date]) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        for month_value in months:
            db.add(
                SentinelBackfillProgressORM(
                    run_id=run_id,
                    year=month_value.year,
                    month=month_value.month,
                    status=STATUS_QUEUED,
                    rows_written=0,
                    updated_at=now,
                )
            )
        db.commit()


def _update_row(
    *,
    run_id: str,
    month_value: date,
    status: str,
    rows_written: int | None = None,
    error_msg: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        row = db.execute(
            select(SentinelBackfillProgressORM).where(
                SentinelBackfillProgressORM.run_id == run_id,
                SentinelBackfillProgressORM.year == month_value.year,
                SentinelBackfillProgressORM.month == month_value.month,
            )
        ).scalar_one_or_none()
        if row is None:
            return

        row.status = status
        row.updated_at = now
        if rows_written is not None:
            row.rows_written = int(rows_written)
        if error_msg is not None:
            row.error_msg = str(error_msg)[:2000]
        if started_at is not None:
            row.started_at = started_at
        if finished_at is not None:
            row.finished_at = finished_at
        db.commit()


def _process_month(*, run_id: str, month_value: date, force: bool) -> None:
    started_at = datetime.now(timezone.utc)
    started_monotonic = time.time()
    _update_row(
        run_id=run_id,
        month_value=month_value,
        status=STATUS_RUNNING,
        started_at=started_at,
        error_msg=None,
    )

    bucket = settings.era5_gcs_bucket
    if not bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    try:
        exists = month_exists(bucket_name=bucket, year=month_value.year, month=month_value.month)
        if exists and not force:
            _update_row(
                run_id=run_id,
                month_value=month_value,
                status=STATUS_SUCCESS,
                rows_written=0,
                finished_at=datetime.now(timezone.utc),
            )
            return

        frame = build_month_rows(month_value=month_value, run_id=run_id)
        gcs_uri = write_month_parquet(
            bucket_name=bucket,
            year=month_value.year,
            month=month_value.month,
            frame=frame,
        )
        _update_row(
            run_id=run_id,
            month_value=month_value,
            status=STATUS_SUCCESS,
            rows_written=len(frame.index),
            finished_at=datetime.now(timezone.utc),
            error_msg=None,
        )
        LOG.info(
            "sentinel_month_complete run_id=%s month=%s rows=%s gcs=%s seconds=%.2f",
            run_id,
            month_value.strftime("%Y-%m"),
            len(frame.index),
            gcs_uri,
            time.time() - started_monotonic,
        )
    except Exception as exc:  # noqa: BLE001
        _update_row(
            run_id=run_id,
            month_value=month_value,
            status=STATUS_FAILED,
            finished_at=datetime.now(timezone.utc),
            error_msg=str(exc),
        )
        LOG.exception("sentinel_month_failed run_id=%s month=%s", run_id, month_value.strftime("%Y-%m"))


def _run_backfill(*, run_id: str, months: list[date], concurrency: int, force: bool) -> None:
    worker_count = max(1, min(int(concurrency), 4))
    with ThreadPoolExecutor(max_workers=worker_count) as ex:
        futures = [ex.submit(_process_month, run_id=run_id, month_value=month, force=force) for month in months]
        for future in as_completed(futures):
            future.result()


def create_backfill_run(*, start: date, end: date, concurrency: int, force: bool) -> dict:
    missing = validate_sentinel_runtime()
    if missing:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message=f"Missing env vars: {', '.join(missing)}")

    try:
        request_access_token()
    except RuntimeError as exc:
        msg = str(exc)
        if "401/403" in msg:
            raise ApiError(
                status_code=401,
                error_code="UNAUTHORIZED",
                message="Sentinel Hub authentication failed (401/403). Check SENTINEL_HUB credentials.",
            ) from exc
        raise ApiError(status_code=503, error_code="UPSTREAM_ERROR", message=msg) from exc

    normalized_start, normalized_end = normalize_range(start, end)
    months = iter_month_starts(normalized_start, normalized_end)
    run_id = f"sentinel_{uuid4().hex[:12]}"
    _insert_pending_rows(run_id=run_id, months=months)

    thread = threading.Thread(
        target=_run_backfill,
        kwargs={
            "run_id": run_id,
            "months": months,
            "concurrency": max(1, int(concurrency)),
            "force": bool(force),
        },
        daemon=False,
    )
    thread.start()
    return {"run_id": run_id, "total_months": len(months)}


def get_latest_status() -> dict:
    with SessionLocal() as db:
        latest_run_id = db.execute(
            select(SentinelBackfillProgressORM.run_id).order_by(desc(SentinelBackfillProgressORM.updated_at)).limit(1)
        ).scalar_one_or_none()
        if latest_run_id is None:
            return {
                "run_id": None,
                "total_months": 0,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "percent_done": 0.0,
                "last_updated": None,
                "failed_months": [],
            }

        rows = db.execute(
            select(SentinelBackfillProgressORM)
            .where(SentinelBackfillProgressORM.run_id == latest_run_id)
            .order_by(SentinelBackfillProgressORM.year, SentinelBackfillProgressORM.month)
        ).scalars().all()

    total_months = len(rows)
    completed = sum(1 for row in rows if row.status == STATUS_SUCCESS)
    failed = sum(1 for row in rows if row.status == STATUS_FAILED)
    running = sum(1 for row in rows if row.status == STATUS_RUNNING)
    done = completed + failed
    percent_done = round((done / total_months) * 100.0, 2) if total_months else 0.0
    last_updated = max((row.updated_at for row in rows), default=None)
    failed_months = [f"{int(row.year):04d}-{int(row.month):02d}" for row in rows if row.status == STATUS_FAILED]

    return {
        "run_id": str(latest_run_id),
        "total_months": total_months,
        "completed": completed,
        "failed": failed,
        "running": running,
        "percent_done": percent_done,
        "last_updated": last_updated.isoformat() if last_updated else None,
        "failed_months": failed_months,
    }


def run_previous_month() -> dict:
    today = datetime.now(timezone.utc).date()
    prev_month_end = date(today.year, today.month, 1) - timedelta(days=1)
    prev_month_start = date(prev_month_end.year, prev_month_end.month, 1)
    return create_backfill_run(start=prev_month_start, end=prev_month_end, concurrency=2, force=False)
