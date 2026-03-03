from __future__ import annotations

from datetime import date, datetime, timezone

from app.errors import ApiError
from pipeline.earthquake_ingestion import (
    earthquake_job_to_status_payload,
    get_earthquake_job,
    get_latest_earthquake_job,
    run_earthquake_daily_update,
    submit_earthquake_backfill,
)


def create_backfill(*, start: date, end: date, min_magnitude: float) -> dict:
    try:
        job_id, deduped, effective_end = submit_earthquake_backfill(
            start_date=start,
            end_date=end,
            min_magnitude=min_magnitude,
        )
    except ValueError as exc:
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message=str(exc)) from exc
    except RuntimeError as exc:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message=str(exc)) from exc

    days_total = (effective_end - start).days + 1
    progress = {
        "days_total": max(days_total, 0),
        "days_done": 0,
        "days_failed": 0,
        "failed_days": [],
        "rows_written_total": 0,
        "files_written_total": 0,
        "last_day_written": None,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "effective_end": effective_end.isoformat(),
    }

    if deduped:
        existing = get_earthquake_job(job_id)
        if existing is not None:
            return earthquake_job_to_status_payload(existing)

    return {
        "job_id": job_id,
        "status": "queued",
        "type": "earthquakes_backfill",
        "created_at": datetime.now(timezone.utc),
        "updated_at": None,
        "progress": progress,
        "children": [],
    }


def get_status() -> dict:
    latest = get_latest_earthquake_job()
    if latest is None:
        raise ApiError(status_code=404, error_code="NOT_FOUND", message="No earthquake ingest job found")
    return earthquake_job_to_status_payload(latest)


def run_daily(*, min_magnitude: float = 2.5) -> dict:
    try:
        return run_earthquake_daily_update(min_magnitude=min_magnitude)
    except RuntimeError as exc:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message=str(exc)) from exc
