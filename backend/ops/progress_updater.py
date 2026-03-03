from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone

from google.cloud import storage
from sqlalchemy import select

from app.database import SessionLocal
from app.orm import BackfillProgressORM


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _parse_month_start(value: str, default_value: date) -> date:
    if not value:
        return default_value
    parsed = datetime.fromisoformat(value).date()
    return date(parsed.year, parsed.month, 1)


def _month_range(start: date, end: date) -> list[date]:
    months: list[date] = []
    cur = start
    while cur <= end:
        months.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return months


def _read_existing_payload(blob: storage.Blob, total_months: int) -> dict:
    if not blob.exists():
        return {
            "run_id": "awsbf_unknown",
            "total_months": total_months,
            "completed": 0,
            "failed": 0,
            "running": 0,
            "percent_done": 0.0,
            "failed_months": [],
            "eta_hours": None,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
    try:
        return json.loads(blob.download_as_text())
    except Exception:
        return {
            "run_id": "awsbf_unknown",
            "total_months": total_months,
            "completed": 0,
            "failed": 0,
            "running": 0,
            "percent_done": 0.0,
            "failed_months": [],
            "eta_hours": None,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }


def _build_payload(start: date, end: date, month_set: set[date]) -> dict:
    with SessionLocal() as db:
        rows = db.execute(
            select(
                BackfillProgressORM.month,
                BackfillProgressORM.status,
                BackfillProgressORM.run_id,
            ).where(
                BackfillProgressORM.month >= start,
                BackfillProgressORM.month <= end,
            )
        ).all()

    status_by_month = {row[0]: row[1] for row in rows}
    run_ids = [row[2] for row in rows if row[2]]
    run_id = run_ids[-1] if run_ids else "awsbf_unknown"
    completed = sum(1 for month in month_set if status_by_month.get(month) == "success")
    failed = sum(1 for month in month_set if status_by_month.get(month) == "failed")
    running = sum(1 for month in month_set if status_by_month.get(month) == "running")
    total = len(month_set)
    failed_months = [month.strftime("%Y-%m") for month in sorted(month_set) if status_by_month.get(month) == "failed"]
    percent = round((completed / total) * 100.0, 2) if total else 100.0

    return {
        "run_id": run_id,
        "total_months": total,
        "completed": completed,
        "failed": failed,
        "running": running,
        "percent_done": percent,
        "failed_months": failed_months,
        "eta_hours": None,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    _require_env("DATABASE_URL")
    bucket_name = _require_env("ERA5_GCS_BUCKET")

    start = _parse_month_start(os.getenv("BACKFILL_START", ""), date(2015, 1, 1))
    end = _parse_month_start(os.getenv("BACKFILL_END", ""), date(2024, 12, 1))
    interval_sec = int(os.getenv("PROGRESS_UPDATE_INTERVAL_SEC", "60"))
    max_loops = int(os.getenv("PROGRESS_UPDATE_MAX_LOOPS", str(24 * 60)))

    month_set = set(_month_range(start, end))

    client = storage.Client()
    blob = client.bucket(bucket_name).blob("backfill-status/progress.json")
    last_payload = _read_existing_payload(blob, total_months=len(month_set))

    for _ in range(max_loops):
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            payload = _build_payload(start, end, month_set)
            last_payload = payload
        except Exception as exc:
            payload = dict(last_payload)
            payload["last_updated"] = now_iso
            payload["updater_error"] = str(exc)[:300]

        try:
            blob.upload_from_string(json.dumps(payload), content_type="application/json")
        except Exception:
            pass

        if int(payload.get("running", 0)) == 0:
            break
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()

