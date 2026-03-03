from __future__ import annotations

from fastapi import APIRouter, Header

from app.config import settings
from app.errors import ApiError
from app.services.job_service import run_openaq_daily_update

router = APIRouter()


@router.post("/cron/openaq/daily", tags=["Jobs"])
async def openaq_daily_update(x_cron_secret: str | None = Header(default=None)):
    if not settings.cron_secret:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid cron secret")
    return run_openaq_daily_update()
