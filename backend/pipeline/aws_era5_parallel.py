from __future__ import annotations

from datetime import date, datetime, timezone
import json
import time
from uuid import uuid4

from google.cloud import storage
import pandas as pd
from sqlalchemy import select

from app.config import settings
from app.database import SessionLocal
from app.orm import BackfillProgressORM
from pipeline.aws_era5_ingestion import process_single_month_features

try:
    from dask.distributed import Client, LocalCluster, as_completed
except Exception:  # noqa: BLE001
    Client = None  # type: ignore[assignment]
    LocalCluster = None  # type: ignore[assignment]
    as_completed = None  # type: ignore[assignment]

DEFAULT_VARIABLES = [
    "2m_temperature",
    "total_precipitation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "volumetric_soil_water_layer_1",
]


def _month_start(value: pd.Timestamp | date | str) -> date:
    if isinstance(value, pd.Timestamp):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    parsed = pd.Timestamp(value)
    return date(parsed.year, parsed.month, 1)


def is_month_completed(month: pd.Timestamp | date | str) -> bool:
    m = _month_start(month)
    with SessionLocal() as db:
        row = db.get(BackfillProgressORM, m)
        return bool(row and row.status == "success")


def mark_month_running(month: pd.Timestamp | date | str, run_id: str) -> None:
    m = _month_start(month)
    with SessionLocal() as db:
        row = db.get(BackfillProgressORM, m)
        if row is None:
            row = BackfillProgressORM(month=m, status="running", run_id=run_id)
            db.add(row)
        else:
            row.status = "running"
            row.run_id = run_id
            row.error_msg = None
        db.commit()


def mark_month_complete(month: pd.Timestamp | date | str, *, row_count: int, duration_sec: float, run_id: str) -> None:
    m = _month_start(month)
    with SessionLocal() as db:
        row = db.get(BackfillProgressORM, m)
        if row is None:
            row = BackfillProgressORM(month=m)
            db.add(row)
        row.status = "success"
        row.row_count = int(row_count)
        row.duration_sec = float(duration_sec)
        row.error_msg = None
        row.completed_at = datetime.now(timezone.utc)
        row.run_id = run_id
        db.commit()


def mark_month_failed(month: pd.Timestamp | date | str, *, error_msg: str, run_id: str) -> None:
    m = _month_start(month)
    with SessionLocal() as db:
        row = db.get(BackfillProgressORM, m)
        if row is None:
            row = BackfillProgressORM(month=m)
            db.add(row)
        row.status = "failed"
        row.error_msg = str(error_msg)[:2000]
        row.run_id = run_id
        row.completed_at = datetime.now(timezone.utc)
        db.commit()


def _build_progress_payload(*, run_id: str, start: date, end: date, started_at: float) -> dict:
    months = pd.date_range(start, end, freq="MS")
    month_dates = {_month_start(m) for m in months}
    with SessionLocal() as db:
        rows = db.execute(select(BackfillProgressORM).where(BackfillProgressORM.month.in_(month_dates))).scalars().all()

    status_by_month = {row.month: row.status for row in rows}
    failed_months = [m.strftime("%Y-%m") for m in sorted(month_dates) if status_by_month.get(m) == "failed"]
    completed = sum(1 for m in month_dates if status_by_month.get(m) == "success")
    failed = len(failed_months)
    running = sum(1 for m in month_dates if status_by_month.get(m) == "running")
    total = len(month_dates)
    percent = round((completed / total) * 100.0, 2) if total else 100.0

    elapsed_hours = max((time.time() - started_at) / 3600.0, 1e-6)
    rate = completed / elapsed_hours
    remaining = max(total - completed - failed, 0)
    eta_hours = round(remaining / rate, 2) if rate > 0 else None

    return {
        "run_id": run_id,
        "total_months": total,
        "completed": completed,
        "failed": failed,
        "running": running,
        "percent_done": percent,
        "failed_months": failed_months,
        "eta_hours": eta_hours,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


def write_progress_json(*, bucket: str, payload: dict) -> None:
    client = storage.Client()
    blob = client.bucket(bucket).blob("backfill-status/progress.json")
    blob.upload_from_string(json.dumps(payload, ensure_ascii=True, default=str), content_type="application/json")


def read_progress_json(*, bucket: str) -> dict:
    client = storage.Client()
    blob = client.bucket(bucket).blob("backfill-status/progress.json")
    if not blob.exists():
        return {
            "total_months": 0,
            "completed": 0,
            "failed": 0,
            "running": 0,
            "percent_done": 0.0,
            "failed_months": [],
            "eta_hours": None,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
    text = blob.download_as_text()
    return json.loads(text)


def _process_month_worker(
    month_iso: str,
    *,
    points_set: str,
    variables: list[str],
    run_id: str,
    processing_mode: str,
) -> dict:
    month = _month_start(month_iso)
    started = time.time()
    mark_month_running(month, run_id)
    try:
        result = process_single_month_features(
            month_start=month,
            variables=variables,
            points_set=points_set,
            run_id=run_id,
            processing_mode=processing_mode,
            worker_id=month.strftime("%Y%m"),
        )
        mark_month_complete(month, row_count=int(result.get("row_count", 0)), duration_sec=time.time() - started, run_id=run_id)
        return {"month": month.strftime("%Y-%m"), "status": "success", **result}
    except Exception as exc:  # noqa: BLE001
        mark_month_failed(month, error_msg=str(exc), run_id=run_id)
        return {"month": month.strftime("%Y-%m"), "status": "failed", "error": str(exc)}


def run_parallel_backfill(
    start: str,
    end: str,
    n_workers: int = 14,
    points_set: str = "provinces",
    variables: list[str] | None = None,
    processing_mode: str = "streaming",
    run_id: str | None = None,
    force: bool = False,
    gcs_bucket: str | None = None,
) -> dict:
    if Client is None or LocalCluster is None or as_completed is None:
        raise RuntimeError("dask[distributed] is required for run_parallel_backfill")

    start_date = _month_start(start)
    end_date = _month_start(end)
    run_id = run_id or f"awsbf_{uuid4().hex[:12]}"
    bucket = gcs_bucket or settings.era5_gcs_bucket
    if not bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    variables = variables or DEFAULT_VARIABLES
    months = list(pd.date_range(start_date, end_date, freq="MS"))
    pending = [m for m in months if force or not is_month_completed(m)]

    progress_started = time.time()
    initial_payload = _build_progress_payload(run_id=run_id, start=start_date, end=end_date, started_at=progress_started)
    write_progress_json(bucket=bucket, payload=initial_payload)

    if not pending:
        final_payload = _build_progress_payload(run_id=run_id, start=start_date, end=end_date, started_at=progress_started)
        write_progress_json(bucket=bucket, payload=final_payload)
        return final_payload

    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit="2GB",
    )
    client = Client(cluster)
    last_progress_flush = time.time()
    results: list[dict] = []

    try:
        futures = client.map(
            _process_month_worker,
            [m.strftime("%Y-%m-01") for m in pending],
            points_set=points_set,
            variables=variables,
            run_id=run_id,
            processing_mode=processing_mode,
        )

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            now = time.time()
            if now - last_progress_flush >= 600:
                payload = _build_progress_payload(run_id=run_id, start=start_date, end=end_date, started_at=progress_started)
                write_progress_json(bucket=bucket, payload=payload)
                last_progress_flush = now

    finally:
        client.close()
        cluster.close()

    final_payload = _build_progress_payload(run_id=run_id, start=start_date, end=end_date, started_at=progress_started)
    final_payload["results"] = results
    write_progress_json(bucket=bucket, payload=final_payload)
    return final_payload


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run AWS ERA5 parallel backfill with streaming")
    parser.add_argument("--start", required=True, help="Start date, e.g. 1950-01-01")
    parser.add_argument("--end", required=True, help="End date, e.g. 2026-12-31")
    parser.add_argument("--workers", type=int, default=14)
    parser.add_argument("--points-set", default="provinces")
    parser.add_argument("--mode", default="streaming", choices=["streaming", "download"])
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    payload = run_parallel_backfill(
        start=args.start,
        end=args.end,
        n_workers=args.workers,
        points_set=args.points_set,
        processing_mode=args.mode,
        force=args.force,
    )
    print(json.dumps(payload, ensure_ascii=True))


if __name__ == "__main__":
    main()
