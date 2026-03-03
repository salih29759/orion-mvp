from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4
import json
import threading

from sqlalchemy import desc, select

from app.database import SessionLocal
from app.orm import DemJobRunORM


def validate_dem_runtime() -> list[str]:
    # Filled in during full implementation.
    return []


def _background_stub(run_id: str) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        row = db.get(DemJobRunORM, run_id)
        if row is None:
            return
        row.status = "running"
        row.started_at = now
        row.updated_at = now
        db.commit()


def create_dem_run(*, include_grid: bool) -> dict:
    run_id = f"dem_{uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)
    progress = {
        "tiles_total": 0,
        "tiles_glo30": 0,
        "tiles_glo90": 0,
        "provinces_total": 0,
        "provinces_done": 0,
        "grid_cells_total": 0,
        "grid_cells_done": 0,
        "warning_count": 0,
    }
    with SessionLocal() as db:
        db.add(
            DemJobRunORM(
                run_id=run_id,
                status="queued",
                include_grid=bool(include_grid),
                progress_json=json.dumps(progress),
                created_at=now,
                updated_at=now,
            )
        )
        db.commit()

    thread = threading.Thread(target=_background_stub, args=(run_id,), daemon=False)
    thread.start()

    return {
        "run_id": run_id,
        "status": "queued",
        "type": "dem_reference_build",
        "created_at": now,
        "progress": progress,
    }


def get_latest_dem_status() -> dict:
    with SessionLocal() as db:
        row = db.execute(select(DemJobRunORM).order_by(desc(DemJobRunORM.updated_at)).limit(1)).scalar_one_or_none()
    if row is None:
        return {
            "run_id": None,
            "status": "idle",
            "type": "dem_reference_build",
            "include_grid": False,
            "created_at": None,
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "progress": {
                "tiles_total": 0,
                "tiles_glo30": 0,
                "tiles_glo90": 0,
                "provinces_total": 0,
                "provinces_done": 0,
                "grid_cells_total": 0,
                "grid_cells_done": 0,
                "warning_count": 0,
            },
            "province_gcs_uri": None,
            "grid_gcs_uri": None,
            "error": None,
        }
    return {
        "run_id": row.run_id,
        "status": row.status,
        "type": "dem_reference_build",
        "include_grid": bool(row.include_grid),
        "created_at": row.created_at,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
        "updated_at": row.updated_at,
        "progress": json.loads(row.progress_json or "{}"),
        "province_gcs_uri": row.province_gcs_uri,
        "grid_gcs_uri": row.grid_gcs_uri,
        "error": row.error,
    }
