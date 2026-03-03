from __future__ import annotations

from fastapi import APIRouter, Depends, Header, status

from app.auth import verify_token
from app.config import settings
from app.errors import ApiError
from app.schemas.sentinel import SentinelBackfillAcceptedResponse, SentinelBackfillRequest, SentinelBackfillStatusResponse
from app.services.sentinel_job_service import create_backfill, get_status, run_monthly_update

router = APIRouter()


def _token_from_bearer_header(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _verify_cron_or_orion_bearer(*, x_cron_secret: str | None, authorization: str | None) -> None:
    token = _token_from_bearer_header(authorization)
    if token is not None and settings.orion_backend_api_key and token == settings.orion_backend_api_key:
        return

    if x_cron_secret is not None:
        if not settings.cron_secret:
            raise ApiError(
                status_code=503,
                error_code="CONFIG_ERROR",
                message="CRON_SECRET is not configured",
            )
        if x_cron_secret == settings.cron_secret:
            return

    if token is not None:
        if not settings.orion_backend_api_key:
            raise ApiError(
                status_code=503,
                error_code="CONFIG_ERROR",
                message="ORION_BACKEND_API_KEY is not configured",
            )
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid ORION backend API key")

    raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid cron credentials")


@router.post("/jobs/sentinel/backfill", response_model=SentinelBackfillAcceptedResponse, status_code=status.HTTP_202_ACCEPTED, tags=["Jobs"])
async def sentinel_backfill(body: SentinelBackfillRequest, _: str = Depends(verify_token)):
    return create_backfill(
        start=body.start,
        end=body.end,
        concurrency=body.concurrency,
        force=body.force,
    )


@router.get("/jobs/sentinel/status", response_model=SentinelBackfillStatusResponse, tags=["Jobs"])
async def sentinel_status(_: str = Depends(verify_token)):
    return get_status()


@router.post("/cron/sentinel/monthly", response_model=SentinelBackfillAcceptedResponse, tags=["Jobs"])
async def sentinel_monthly_update(
    x_cron_secret: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
):
    _verify_cron_or_orion_bearer(x_cron_secret=x_cron_secret, authorization=authorization)
    return run_monthly_update()
