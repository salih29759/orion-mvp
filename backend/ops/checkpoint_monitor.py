from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import date, datetime, timezone

from sqlalchemy import desc, func, select

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


def _run(cmd: str) -> str:
    proc = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _latest_gcs_object(bucket: str) -> str:
    cmd = (
        f"gsutil ls -l 'gs://{bucket}/features/daily/**' "
        "| grep -v '^TOTAL:' | sort -k2 | tail -n 1"
    )
    out = _run(cmd)
    if not out:
        return ""
    parts = out.split()
    return parts[-1] if parts else ""


def _progress_last_updated(bucket: str) -> str:
    out = _run(f"gsutil cat gs://{bucket}/backfill-status/progress.json")
    if not out:
        return ""
    try:
        payload = json.loads(out)
    except Exception:
        return ""
    return str(payload.get("last_updated", ""))


def _snapshot(start: date, end: date, bucket: str) -> dict:
    with SessionLocal() as db:
        status_counts = dict(db.execute(select(BackfillProgressORM.status, func.count()).group_by(BackfillProgressORM.status)).all())
        latest_success = db.execute(
            select(BackfillProgressORM.month, BackfillProgressORM.completed_at)
            .where(
                BackfillProgressORM.status == "success",
                BackfillProgressORM.month >= start,
                BackfillProgressORM.month <= end,
            )
            .order_by(desc(BackfillProgressORM.completed_at))
            .limit(1)
        ).first()

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "success": int(status_counts.get("success", 0)),
        "failed": int(status_counts.get("failed", 0)),
        "running": int(status_counts.get("running", 0)),
        "latest_success_month": latest_success[0].isoformat() if latest_success else None,
        "latest_success_at": latest_success[1].isoformat() if latest_success and latest_success[1] else None,
        "progress_last_updated": _progress_last_updated(bucket),
        "latest_gcs_object": _latest_gcs_object(bucket),
    }


def main() -> None:
    _require_env("DATABASE_URL")
    bucket = _require_env("ERA5_GCS_BUCKET")

    start = _parse_month_start(os.getenv("BACKFILL_START", ""), date(2015, 1, 1))
    end = _parse_month_start(os.getenv("BACKFILL_END", ""), date(2024, 12, 1))
    interval_sec = int(os.getenv("CHECKPOINT_INTERVAL_SEC", "1800"))
    max_loops = int(os.getenv("CHECKPOINT_MAX_LOOPS", "48"))
    log_path = os.getenv("CHECKPOINT_LOG_PATH", "backfill-checkpoints.log")

    for _ in range(max_loops):
        try:
            payload = _snapshot(start, end, bucket)
        except Exception as exc:
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "success": None,
                "failed": None,
                "running": None,
                "latest_success_month": None,
                "latest_success_at": None,
                "progress_last_updated": "",
                "latest_gcs_object": "",
                "monitor_error": str(exc)[:300],
            }

        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")

        if payload.get("running") == 0:
            break
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()

