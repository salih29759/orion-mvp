from __future__ import annotations

from fastapi import APIRouter, Depends, Header, status

from app.auth import verify_token
from app.config import settings
from app.errors import ApiError
from app.schemas.common import JobStatusResponse
from app.schemas.openmeteo import OpenMeteoBackfillRequest, OpenMeteoForecastRequest
from app.services.job_service import (
    create_openmeteo_backfill_job,
    create_openmeteo_forecast_job,
    run_openmeteo_daily_update,
)

router = APIRouter()


def _verify_cron_secret(x_cron_secret: str | None) -> None:
    if not settings.cron_secret:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid cron secret")


@router.post("/jobs/openmeteo/backfill", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED, tags=["Jobs"])
async def openmeteo_backfill(body: OpenMeteoBackfillRequest, _: str = Depends(verify_token)):
    return create_openmeteo_backfill_job(
        start=body.start,
        end=body.end,
        concurrency=body.concurrency,
    )


@router.post("/jobs/openmeteo/forecast", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED, tags=["Jobs"])
async def openmeteo_forecast(body: OpenMeteoForecastRequest, _: str = Depends(verify_token)):
    return create_openmeteo_forecast_job(forecast_days=body.forecast_days)


@router.post("/cron/openmeteo/daily", tags=["Jobs"])
async def openmeteo_daily(
    _: str = Depends(verify_token),
    x_cron_secret: str | None = Header(default=None),
):
    _verify_cron_secret(x_cron_secret)
    return run_openmeteo_daily_update()
