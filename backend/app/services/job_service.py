from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.era5_presets import CORE_VARIABLES
from app.errors import ApiError
from pipeline.era5_ingestion import (
    Era5Request,
    get_backfill_status,
    get_jobs_metrics,
    get_job,
    kick_queued_jobs,
    submit_backfill,
    submit_era5_job,
    validate_era5_runtime,
)
from pipeline.firms_ingestion import (
    FirmsRequest,
    get_asset_wildfire_features,
    get_firms_job,
    get_firms_metrics,
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


def get_job_status_payload(job_id: str) -> dict:
    bf = get_backfill_status(job_id, include_items=True)
    if bf:
        status = "success" if bf.get("status") == "finished" else bf.get("status")
        return {
            "job_id": bf["backfill_id"],
            "status": status,
            "type": "era5_backfill",
            "created_at": bf.get("created_at"),
            "updated_at": bf.get("finished_at"),
            "progress": {
                "months_total": bf.get("months_total", 0),
                "months_success": bf.get("months_success", 0),
                "months_failed": bf.get("months_failed", 0),
                "failed_months": bf.get("failed_months", []),
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
        "type": "era5_ingest",
        "created_at": job.created_at,
        "updated_at": job.finished_at or job.started_at,
        "progress": {
            "rows_written": job.rows_written,
            "bytes_downloaded": job.bytes_downloaded,
            "dq_status": job.dq_status,
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

