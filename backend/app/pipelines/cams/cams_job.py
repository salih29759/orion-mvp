from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
import logging
from pathlib import Path
import tempfile
import threading
import time
from uuid import uuid4

from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import CamsBackfillProgressORM
from app.pipelines.cams.cams_client import (
    CAMS_DATASET,
    CamsAvailability,
    build_availability,
    download_month_netcdf,
    fetch_ads_constraints,
    iter_month_starts,
    month_start,
    pick_reanalysis_type,
    resolve_effective_end,
)
from app.pipelines.cams.cams_extract import extract_daily_grid
from app.pipelines.cams.cams_sample import load_province_points, sample_daily_to_provinces
from app.pipelines.cams.cams_writer import month_object_exists, write_month_parquet

LOG = logging.getLogger("orion.cams.job")

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def should_skip_month(*, month_exists: bool, force: bool) -> bool:
    return month_exists and not force


def _ensure_runtime() -> None:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    has_ads_key = bool(settings.adsapi_key)
    has_cdsapirc = Path.home().joinpath(".cdsapirc").exists()
    if not (has_ads_key or has_cdsapirc):
        raise RuntimeError("ADS credentials missing. Set ADSAPI_KEY or create ~/.cdsapirc")


def _upsert_progress(
    *,
    run_id: str,
    month: date,
    status: str,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    error_msg: str | None = None,
    rows_written: int | None = None,
    duration_sec: float | None = None,
) -> None:
    with SessionLocal() as db:
        row = db.execute(
            select(CamsBackfillProgressORM).where(
                CamsBackfillProgressORM.run_id == run_id,
                CamsBackfillProgressORM.month == month,
            )
        ).scalar_one_or_none()
        if row is None:
            row = CamsBackfillProgressORM(run_id=run_id, month=month, status=status)
            db.add(row)

        row.status = status
        row.updated_at = _now()
        if started_at is not None:
            row.started_at = started_at
        if completed_at is not None:
            row.completed_at = completed_at
        if error_msg is not None:
            row.error_msg = error_msg[:2000]
        if rows_written is not None:
            row.rows_written = int(rows_written)
        if duration_sec is not None:
            row.duration_sec = float(duration_sec)
        db.commit()


def _init_run_rows(run_id: str, months: list[date]) -> None:
    with SessionLocal() as db:
        now = _now()
        for month in months:
            db.add(
                CamsBackfillProgressORM(
                    run_id=run_id,
                    month=month,
                    status=STATUS_PENDING,
                    rows_written=0,
                    updated_at=now,
                )
            )
        db.commit()


def _process_single_month(
    *,
    run_id: str,
    month: date,
    availability: CamsAvailability,
    force: bool,
    source: str,
) -> None:
    started = _now()
    started_ts = time.time()
    _upsert_progress(run_id=run_id, month=month, status=STATUS_RUNNING, started_at=started, error_msg="")

    try:
        if should_skip_month(month_exists=month_object_exists(month), force=force):
            _upsert_progress(
                run_id=run_id,
                month=month,
                status=STATUS_COMPLETED,
                completed_at=_now(),
                rows_written=0,
                duration_sec=time.time() - started_ts,
                error_msg="",
            )
            LOG.info("cams_month_skipped_exists run_id=%s month=%s", run_id, month.strftime("%Y-%m"))
            return

        reanalysis_type = pick_reanalysis_type(month, availability)
        nc_path = Path(tempfile.gettempdir()) / f"orion_cams_{run_id}_{month.year:04d}_{month.month:02d}.nc"
        download_month_netcdf(
            year=month.year,
            month=month.month,
            reanalysis_type=reanalysis_type,
            target_path=nc_path,
        )

        daily_grid, unit_notes = extract_daily_grid(nc_path)
        provinces = load_province_points()
        daily = sample_daily_to_provinces(
            daily_grid=daily_grid,
            provinces=provinces,
            source=source,
            run_id=run_id,
            ingested_at=_now(),
        )

        uri = write_month_parquet(daily, month, overwrite=True)
        duration = time.time() - started_ts
        _upsert_progress(
            run_id=run_id,
            month=month,
            status=STATUS_COMPLETED,
            completed_at=_now(),
            rows_written=int(len(daily)),
            duration_sec=duration,
            error_msg="",
        )
        LOG.info(
            "cams_month_completed run_id=%s month=%s type=%s rows=%d duration_sec=%.2f uri=%s units=%s",
            run_id,
            month.strftime("%Y-%m"),
            reanalysis_type,
            len(daily),
            duration,
            uri,
            unit_notes,
        )
    except Exception as exc:  # noqa: BLE001
        _upsert_progress(
            run_id=run_id,
            month=month,
            status=STATUS_FAILED,
            completed_at=_now(),
            duration_sec=time.time() - started_ts,
            error_msg=str(exc),
        )
        LOG.exception("cams_month_failed run_id=%s month=%s error=%s", run_id, month.strftime("%Y-%m"), str(exc))


def _run_backfill(
    *,
    run_id: str,
    months: list[date],
    availability: CamsAvailability,
    concurrency: int,
    force: bool,
    source: str,
) -> None:
    workers = min(max(concurrency, 1), 2)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _process_single_month,
                run_id=run_id,
                month=month,
                availability=availability,
                force=force,
                source=source,
            )
            for month in months
        ]
        for fut in as_completed(futures):
            fut.result()


def create_backfill(
    *,
    start: date,
    end: date,
    concurrency: int,
    force: bool,
) -> dict:
    _ensure_runtime()
    requested_start = month_start(start)

    constraints = fetch_ads_constraints()
    availability = build_availability(constraints)
    effective_end = resolve_effective_end(end, availability)
    if requested_start > effective_end:
        raise RuntimeError(
            f"No CAMS data available for start={requested_start:%Y-%m}; latest available is {effective_end:%Y-%m}"
        )

    months = iter_month_starts(requested_start, effective_end)
    run_id = f"cams_{uuid4().hex[:12]}"
    _init_run_rows(run_id, months)

    source = f"cams_{CAMS_DATASET}"
    thread = threading.Thread(
        target=_run_backfill,
        kwargs={
            "run_id": run_id,
            "months": months,
            "availability": availability,
            "concurrency": concurrency,
            "force": force,
            "source": source,
        },
        daemon=False,
    )
    thread.start()

    return {
        "run_id": run_id,
        "status": "queued",
        "type": "cams_backfill",
        "created_at": _now(),
        "progress": {
            "requested_start": requested_start.isoformat(),
            "requested_end": month_start(end).isoformat(),
            "effective_start": requested_start.isoformat(),
            "effective_end": effective_end.isoformat(),
            "months_total": len(months),
            "completed": 0,
            "failed": 0,
            "running": 0,
            "pending": len(months),
        },
    }


def _summary_from_rows(run_id: str, rows: list[CamsBackfillProgressORM]) -> dict:
    total = len(rows)
    completed = sum(1 for row in rows if row.status == STATUS_COMPLETED)
    failed = sum(1 for row in rows if row.status == STATUS_FAILED)
    running = sum(1 for row in rows if row.status == STATUS_RUNNING)
    pending = sum(1 for row in rows if row.status == STATUS_PENDING)
    percent = round((((completed + failed) / total) * 100.0), 2) if total else 0.0

    last_updated_dt = max((row.updated_at for row in rows), default=_now())
    recent_error_rows = [row for row in rows if row.status == STATUS_FAILED and row.error_msg]
    recent_error_rows.sort(key=lambda row: row.updated_at or _now(), reverse=True)
    recent_errors = [f"{row.month:%Y-%m}: {row.error_msg}" for row in recent_error_rows[:5]]

    if total == 0:
        run_status = "not_found"
    elif completed + failed == total:
        run_status = "completed" if failed == 0 else "completed_with_errors"
    elif running > 0:
        run_status = "running"
    else:
        run_status = "pending"

    return {
        "run_id": run_id,
        "total_months": total,
        "completed": completed,
        "failed": failed,
        "running": running,
        "pending": pending,
        "percent_done": percent,
        "last_updated": last_updated_dt.isoformat(),
        "recent_errors": recent_errors,
        "status": run_status,
    }


def get_status(run_id: str | None = None) -> dict:
    with SessionLocal() as db:
        selected_run = run_id
        if not selected_run:
            selected_run = db.execute(
                select(CamsBackfillProgressORM.run_id)
                .order_by(desc(CamsBackfillProgressORM.updated_at))
                .limit(1)
            ).scalar_one_or_none()

        if not selected_run:
            return {
                "run_id": None,
                "total_months": 0,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "pending": 0,
                "percent_done": 0.0,
                "last_updated": _now().isoformat(),
                "recent_errors": [],
                "status": "idle",
            }

        rows = db.execute(
            select(CamsBackfillProgressORM)
            .where(CamsBackfillProgressORM.run_id == selected_run)
            .order_by(CamsBackfillProgressORM.month)
        ).scalars().all()

    return _summary_from_rows(selected_run, rows)
