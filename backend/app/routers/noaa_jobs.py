from __future__ import annotations

from fastapi import APIRouter, Depends, Header, status
from fastapi.security import HTTPAuthorizationCredentials

from app.auth import verify_token
from app.config import settings
from app.errors import ApiError
from app.schemas.noaa import NoaaBackfillAcceptedResponse, NoaaBackfillRequest, NoaaBackfillStatusResponse
from app.services.noaa_job_service import create_backfill, get_status, run_daily

router = APIRouter()


def _authorize_cron_or_bearer(*, x_cron_secret: str | None, authorization: str | None) -> None:
    if settings.cron_secret and x_cron_secret == settings.cron_secret:
        return

    credentials: HTTPAuthorizationCredentials | None = None
    if authorization:
        parts = authorization.split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip():
            credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=parts[1].strip())

    verify_token(credentials)


@router.post("/jobs/noaa/backfill", response_model=NoaaBackfillAcceptedResponse, status_code=status.HTTP_202_ACCEPTED, tags=["Jobs"])
async def noaa_backfill(body: NoaaBackfillRequest, _: str = Depends(verify_token)):
    return create_backfill(start=body.start, end=body.end, concurrency=body.concurrency, force=body.force)


@router.get("/jobs/noaa/status", response_model=NoaaBackfillStatusResponse, tags=["Jobs"])
async def noaa_status(_: str = Depends(verify_token)):
    return get_status()


@router.post("/cron/noaa/daily", tags=["Jobs"])
async def noaa_daily(
    x_cron_secret: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
):
    try:
        _authorize_cron_or_bearer(x_cron_secret=x_cron_secret, authorization=authorization)
    except ApiError as exc:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message=exc.message) from exc

    return run_daily()
