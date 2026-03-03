from __future__ import annotations

from fastapi import APIRouter, Depends, Header

from app.auth import verify_token
from app.config import settings
from app.errors import ApiError
from app.schemas.aws import AwsCatalogLatestResponse, AwsCatalogSyncRequest, AwsEra5BackfillRequest, AwsEra5StatusResponse
from app.schemas.common import JobStatusResponse
from app.services.aws_job_service import create_backfill, create_catalog_sync, get_catalog_latest, get_latest_status
from app.services.orchestration_service import enqueue_aws_monthly_update

router = APIRouter()


@router.post("/jobs/aws-era5/catalog/sync", response_model=JobStatusResponse, tags=["Jobs"])
async def aws_catalog_sync(body: AwsCatalogSyncRequest, _: str = Depends(verify_token)):
    return create_catalog_sync(prefixes=body.prefixes, max_keys_per_prefix=body.max_keys_per_prefix)


@router.get("/jobs/aws-era5/catalog/latest", response_model=AwsCatalogLatestResponse, tags=["Jobs"])
async def aws_catalog_latest(_: str = Depends(verify_token)):
    return get_catalog_latest()


@router.post("/jobs/aws-era5/backfill", response_model=JobStatusResponse, status_code=202, tags=["Jobs"])
async def aws_backfill(body: AwsEra5BackfillRequest, _: str = Depends(verify_token)):
    return create_backfill(
        start=body.start,
        end=body.end,
        mode=body.resolved_processing_mode(),
        extraction_mode=body.resolved_extraction_mode(),
        points_set=body.points_set,
        bbox=body.bbox,
        variables=body.variables,
        concurrency=body.concurrency,
        n_workers=body.n_workers,
        force=body.force,
    )


@router.get("/jobs/aws-era5/status", response_model=AwsEra5StatusResponse, tags=["Jobs"])
async def aws_backfill_status(_: str = Depends(verify_token)):
    return get_latest_status()


@router.post("/cron/aws-era5/monthly-update", tags=["Jobs"])
async def aws_monthly_update(
    x_cron_secret: str | None = Header(default=None),
):
    if not settings.cron_secret:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid cron secret")

    return enqueue_aws_monthly_update()
