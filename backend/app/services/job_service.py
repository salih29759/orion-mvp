from __future__ import annotations

from datetime import date, datetime, timezone

from app.config import settings
from app.era5_presets import CORE_VARIABLES
from app.errors import ApiError
from pipeline.aws_era5_catalog import get_catalog_run, get_latest_available, sync_catalog
from pipeline.aws_era5_parallel import read_progress_json
from pipeline.era5_ingestion import (
    get_backfill_status,
    get_jobs_metrics,
    get_job,
    submit_backfill,
    validate_era5_runtime,
)
from pipeline.firms_ingestion import (
    FirmsRequest,
    get_asset_wildfire_features,
    get_firms_job,
    run_daily_firms_update,
    submit_firms_ingest,
)
from pipeline.risk_scoring import build_climatology


def create_backfill_job(*, start_month: str, end_month: str, bbox: dict, variables: list[str], mode: str, concurrency: int):
    missing = validate_era5_runtime()
    if missing:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message=f"Missing env vars: {', '.join(missing)}")
    backfill_id, _, months_total = submit_backfill(
        start_month=start_month,
        end_month=end_month,
        bbox=(bbox["north"], bbox["west"], bbox["south"], bbox["east"]),
        variables=variables,
        mode=mode,
        dataset="era5-land",
        concurrency=concurrency,
        provider_strategy="aws_first_hybrid",
        force=False,
    )
    now = datetime.now(timezone.utc)
    return {
        "job_id": backfill_id,
        "status": "queued",
        "type": "era5_backfill",
        "created_at": now,
        "updated_at": None,
        "progress": {"months_total": months_total, "months_success": 0, "months_failed": 0},
        "children": [],
    }


def create_aws_backfill_job(
    *,
    start: date,
    end: date,
    mode: str,
    extraction_mode: str,
    points_set: str | None,
    bbox: dict,
    variables: list[str],
    concurrency: int,
    n_workers: int,
    force: bool,
) -> dict:
    start_month = f"{start.year:04d}-{start.month:02d}"
    end_month = f"{end.year:04d}-{end.month:02d}"
    total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    pending_months = max(total_months, 0)
    estimated_hours = round((pending_months * 2.0) / max(n_workers, 1), 2)
    backfill_id, _, months_total = submit_backfill(
        start_month=start_month,
        end_month=end_month,
        bbox=(bbox["north"], bbox["west"], bbox["south"], bbox["east"]),
        variables=variables,
        mode="monthly",
        dataset="era5-land",
        concurrency=concurrency,
        provider_strategy="aws_first_hybrid",
        force=force,
        processing_mode=mode,
        points_set=points_set,
        extraction_mode=extraction_mode,
    )
    return {
        "job_id": backfill_id,
        "status": "queued",
        "type": "aws_era5_backfill",
        "created_at": datetime.now(timezone.utc),
        "updated_at": None,
        "estimated_hours": estimated_hours,
        "progress": {
            "months_total": months_total,
            "months_success": 0,
            "months_failed": 0,
            "mode": mode,
            "extraction_mode": extraction_mode,
            "points_set": points_set,
            "n_workers": n_workers,
            "percent_done": 0.0,
            "eta_hours": estimated_hours,
        },
        "children": [],
    }


def create_aws_catalog_sync_job(*, prefixes: list[str] | None, max_keys_per_prefix: int) -> dict:
    out = sync_catalog(prefixes=prefixes, max_keys_per_prefix=max_keys_per_prefix)
    return {
        "job_id": out["run_id"],
        "status": out["status"],
        "type": "aws_era5_catalog_sync",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "progress": {
            "objects_scanned": out["objects_scanned"],
            "prefixes": out["prefixes"],
            "error": out.get("error"),
        },
        "children": [],
    }


def get_aws_catalog_latest() -> dict:
    return get_latest_available(required_variables=CORE_VARIABLES)


def get_aws_latest_status() -> dict:
    if not settings.era5_gcs_bucket:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message="ERA5_GCS_BUCKET is missing")
    try:
        return read_progress_json(bucket=settings.era5_gcs_bucket)
    except Exception as exc:  # noqa: BLE001
        raise ApiError(status_code=503, error_code="STATUS_UNAVAILABLE", message=str(exc)) from exc


def run_aws_monthly_update(*, bbox: dict, variables: list[str], concurrency: int = 2) -> dict:
    latest = get_latest_available(required_variables=variables)
    latest_month = latest.get("latest_common_month")
    if not latest_month:
        sync_catalog(prefixes=None, max_keys_per_prefix=2000)
        latest = get_latest_available(required_variables=variables)
        latest_month = latest.get("latest_common_month")
    if not latest_month:
        raise ApiError(status_code=503, error_code="CATALOG_EMPTY", message="AWS ERA5 catalog has no discoverable month yet")

    backfill_id, _, months_total = submit_backfill(
        start_month=latest_month,
        end_month=latest_month,
        bbox=(bbox["north"], bbox["west"], bbox["south"], bbox["east"]),
        variables=variables,
        mode="monthly",
        dataset="era5-land",
        concurrency=concurrency,
        provider_strategy="aws_first_hybrid",
        force=False,
    )
    return {
        "status": "accepted",
        "job_id": backfill_id,
        "months_total": months_total,
        "latest_common_month": latest_month,
    }


def get_job_status_payload(job_id: str) -> dict:
    catalog_run = get_catalog_run(job_id)
    if catalog_run:
        return {
            "job_id": catalog_run.run_id,
            "status": catalog_run.status,
            "type": "aws_era5_catalog_sync",
            "created_at": catalog_run.started_at,
            "updated_at": catalog_run.finished_at or catalog_run.started_at,
            "progress": {
                "objects_scanned": catalog_run.objects_scanned,
                "error": catalog_run.error,
            },
            "children": [],
        }

    bf = get_backfill_status(job_id, include_items=True)
    if bf:
        status = "success" if bf.get("status") == "finished" else bf.get("status")
        provider_strategy = bf.get("provider_strategy")
        months_total = int(bf.get("months_total", 0) or 0)
        months_success = int(bf.get("months_success", 0) or 0)
        months_failed = int(bf.get("months_failed", 0) or 0)
        percent_done = round(((months_success + months_failed) / months_total) * 100.0, 2) if months_total else 0.0
        created_at = bf.get("created_at")
        eta_hours = None
        if created_at and months_success > 0:
            if isinstance(created_at, datetime):
                created_dt = created_at
            else:
                created_dt = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            elapsed_hours = max((datetime.now(timezone.utc) - created_dt).total_seconds() / 3600.0, 1e-6)
            rate = months_success / elapsed_hours
            remaining = max(months_total - months_success - months_failed, 0)
            eta_hours = round(remaining / rate, 2) if rate > 0 else None
        return {
            "job_id": bf["backfill_id"],
            "status": status,
            "type": "aws_era5_backfill" if provider_strategy == "aws_first_hybrid" else "era5_backfill",
            "created_at": created_at,
            "updated_at": bf.get("finished_at"),
            "estimated_hours": eta_hours,
            "progress": {
                "months_total": months_total,
                "months_success": months_success,
                "months_failed": months_failed,
                "failed_months": bf.get("failed_months", []),
                "percent_done": percent_done,
                "eta_hours": eta_hours,
            },
            "children": [c["job_id"] for c in (bf.get("child_jobs") or []) if c.get("job_id")],
        }

    firms = get_firms_job(job_id)
    if firms:
        return {
            "job_id": firms.job_id,
            "status": firms.status,
            "type": "firms_ingest",
            "created_at": firms.created_at,
            "updated_at": firms.finished_at or firms.started_at,
            "progress": {
                "rows_fetched": firms.rows_fetched,
                "rows_inserted": firms.rows_inserted,
                "raw_gcs_uri": firms.raw_gcs_uri,
            },
            "children": [],
        }

    job = get_job(job_id)
    if not job:
        raise ApiError(status_code=404, error_code="NOT_FOUND", message=f"Job '{job_id}' not found")

    return {
        "job_id": job.job_id,
        "status": job.status,
        "type": "aws_era5_ingest" if (job.provider or "cds") == "aws_nsf_ncar" else "era5_ingest",
        "created_at": job.created_at,
        "updated_at": job.finished_at or job.started_at,
        "progress": {
            "rows_written": job.rows_written,
            "bytes_downloaded": job.bytes_downloaded,
            "dq_status": job.dq_status,
            "provider": job.provider,
            "mode": job.mode,
            "month_label": job.month_label,
        },
        "children": [],
    }


def build_climatology_job(*, version: str, baseline_start, baseline_end, level: str) -> dict:
    out = build_climatology(
        baseline_start=baseline_start,
        baseline_end=baseline_end,
        climatology_version=version,
        level=level,
    )
    return {"version": out["climatology_version"], "status": "success", "row_count": out["row_count"]}


def create_firms_ingest_job(*, source: str, bbox: dict, start_date, end_date) -> dict:
    req = FirmsRequest(
        source=source,
        bbox=(bbox["north"], bbox["west"], bbox["south"], bbox["east"]),
        start_date=start_date,
        end_date=end_date,
    )
    job_id, _ = submit_firms_ingest(req)
    now = datetime.now(timezone.utc)
    return {
        "job_id": job_id,
        "status": "queued",
        "type": "firms_ingest",
        "created_at": now,
        "updated_at": None,
        "progress": {"rows_fetched": 0, "rows_inserted": 0},
        "children": [],
    }


def run_firms_daily_update() -> dict:
    job_id, dedup, start_date, end_date = run_daily_firms_update()
    return {
        "status": "accepted",
        "job_id": job_id,
        "deduplicated": dedup,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def get_metrics_payload() -> dict:
    era5 = get_jobs_metrics(24)
    return {
        "jobs_last_24h": era5["jobs_last_24h"],
        "success_rate": era5["success_rate"],
        "avg_duration_seconds": era5["avg_duration"],
        "bytes_downloaded_last_24h": era5["bytes_downloaded"],
    }


def get_wildfire_features(asset_id: str, window: str) -> dict:
    features = get_asset_wildfire_features(asset_id, window)
    if features is None:
        raise ApiError(status_code=404, error_code="NOT_FOUND", message=f"Asset '{asset_id}' not found")
    return {"status": "success", "asset_id": asset_id, "window": window, **features}
