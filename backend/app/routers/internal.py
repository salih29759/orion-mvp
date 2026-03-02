from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Header, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.orm import Era5IngestJobORM, PipelineRunORM
from pipeline.cds_client import run_cds_smoke_test
from pipeline.run_pipeline import run as run_pipeline

router = APIRouter()


def _verify_cron_secret(x_cron_secret: str | None) -> None:
    if not settings.cron_secret:
        raise HTTPException(status_code=503, detail="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise HTTPException(status_code=401, detail="Invalid cron secret")


@router.post("/pipeline/run", summary="Trigger data pipeline run (scheduler)")
async def trigger_pipeline(
    backfill_days: int | None = Query(None, ge=1, le=30),
    x_cron_secret: str | None = Header(default=None),
):
    _verify_cron_secret(x_cron_secret)

    with SessionLocal() as db:
        running = db.execute(
            select(PipelineRunORM)
            .where(PipelineRunORM.status == "running")
            .order_by(desc(PipelineRunORM.started_at))
            .limit(1)
        ).scalar_one_or_none()
        if running:
            raise HTTPException(status_code=409, detail=f"Pipeline already running (run_id={running.run_id})")

    started_at = datetime.now(timezone.utc)
    days = backfill_days if backfill_days is not None else settings.daily_backfill_days

    run_pipeline(backfill_days=days)

    with SessionLocal() as db:
        latest = db.execute(select(PipelineRunORM).order_by(desc(PipelineRunORM.started_at)).limit(1)).scalar_one_or_none()

    return {
        "status": "success",
        "triggered_at": started_at.isoformat(),
        "backfill_days": days,
        "run_id": latest.run_id if latest else None,
        "run_status": latest.status if latest else "unknown",
        "rows_written": latest.rows_written if latest else 0,
    }


@router.get("/pipeline/status", summary="Get latest pipeline status")
async def pipeline_status(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)

    with SessionLocal() as db:
        latest = db.execute(select(PipelineRunORM).order_by(desc(PipelineRunORM.started_at)).limit(1)).scalar_one_or_none()
        if not latest:
            return {"status": "empty", "message": "No pipeline runs found yet"}
        return {
            "status": "success",
            "run_id": latest.run_id,
            "started_at": latest.started_at.isoformat() if latest.started_at else None,
            "finished_at": latest.finished_at.isoformat() if latest.finished_at else None,
            "run_status": latest.status,
            "rows_written": latest.rows_written,
            "error": latest.error,
        }


@router.post("/cds/test", summary="Run CDS/ERA5 API smoke test")
async def cds_test(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)
    return run_cds_smoke_test()


@router.post("/jobs/recover", summary="Mark stale ERA5 jobs as failed")
async def recover_stale_jobs(
    stale_minutes: int = Query(30, ge=5, le=1440),
    x_cron_secret: str | None = Header(default=None),
):
    _verify_cron_secret(x_cron_secret)
    now = datetime.now(timezone.utc)
    before = now - timedelta(minutes=stale_minutes)

    with SessionLocal() as db:
        rows = db.execute(
            select(Era5IngestJobORM).where(
                Era5IngestJobORM.status.in_(["queued", "running"]),
                Era5IngestJobORM.created_at < before,
            )
        ).scalars().all()
        updated = 0
        for row in rows:
            row.status = "failed"
            row.finished_at = now
            row.error = "manual stale recovery"
            updated += 1
        if updated:
            db.commit()

    return {"status": "success", "recovered_jobs": updated, "stale_minutes": stale_minutes}
