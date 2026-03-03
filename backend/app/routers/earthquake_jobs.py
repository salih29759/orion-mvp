from __future__ import annotations

from fastapi import APIRouter, Depends, Header

from app.auth import verify_token
from app.config import settings
from app.errors import ApiError
from app.schemas.common import JobStatusResponse
from app.schemas.earthquake import EarthquakeBackfillRequest
from app.services.earthquake_job_service import create_backfill, get_status, run_daily

router = APIRouter()


@router.post("/jobs/earthquakes/backfill", response_model=JobStatusResponse, status_code=202, tags=["Jobs"])
async def earthquakes_backfill(body: EarthquakeBackfillRequest, _: str = Depends(verify_token)):
    return create_backfill(start=body.start, end=body.end, min_magnitude=body.min_magnitude)


@router.get("/jobs/earthquakes/status", response_model=JobStatusResponse, tags=["Jobs"])
async def earthquakes_status(_: str = Depends(verify_token)):
    return get_status()


@router.post("/cron/earthquakes/daily", tags=["Jobs"])
async def earthquakes_daily(
    _: str = Depends(verify_token),
    x_cron_secret: str | None = Header(default=None),
):
    if not settings.cron_secret:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid cron secret")

    return run_daily(min_magnitude=2.5)
