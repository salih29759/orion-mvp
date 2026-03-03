from __future__ import annotations

from fastapi import APIRouter, Header

from app.config import settings
from app.errors import ApiError
from app.services.orchestration_service import enqueue_firms_daily_update

router = APIRouter()


def _verify_cron_secret(x_cron_secret: str | None) -> None:
    if not settings.cron_secret:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid cron secret")


@router.post("/cron/firms/daily-update", tags=["FIRMS"])
async def firms_daily_update(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)
    return enqueue_firms_daily_update()
