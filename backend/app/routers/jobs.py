from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query, status

from app.auth import verify_token
from app.errors import ApiError
from app.models import (
    ClimatePoint,
    ClimateSeriesResponse,
    Era5IngestRequest,
    Era5IngestResponse,
)
from app.schemas.common import BBox, BackfillRequest, JobStatusResponse
from app.schemas.glofas import GlofasBackfillRequest, GlofasStatusResponse
from app.services.job_service import (
    create_backfill_job,
    create_firms_ingest_job,
    create_glofas_backfill_job,
    get_glofas_status_payload,
    get_job_status_payload,
)
from pipeline.era5_ingestion import (
    Era5Request,
    get_era5_features,
    kick_queued_jobs,
    request_signature,
    submit_era5_job,
    validate_era5_runtime,
)
from pipeline.open_meteo_series import fetch_open_meteo_daily, fetch_open_meteo_today

router = APIRouter()


@router.post("/era5/backfill", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def era5_backfill(body: BackfillRequest, _: str = Depends(verify_token)):
    return create_backfill_job(
        start_month=body.start_month,
        end_month=body.end_month,
        bbox=body.bbox.model_dump(),
        variables=body.variables,
        mode=body.mode,
        concurrency=body.concurrency,
    )


@router.post(
    "/era5/ingest",
    response_model=Era5IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue an asynchronous ERA5 ingestion job",
)
async def queue_era5_ingest(
    body: Era5IngestRequest,
    _: str = Depends(verify_token),
):
    # Legacy endpoint kept for compatibility.
    missing = validate_era5_runtime()
    if missing:
        raise ApiError(
            status_code=503,
            error_code="CONFIG_ERROR",
            message=f"ERA5 ingestion is not configured. Missing env vars: {', '.join(missing)}",
        )
    req = Era5Request(
        start_date=body.start_date,
        end_date=body.end_date,
        bbox=(body.bbox["north"], body.bbox["west"], body.bbox["south"], body.bbox["east"]),
        variables=body.variables,
        dataset=body.dataset,
        out_format=body.format,
    )
    sig = request_signature(req)
    try:
        job_id, deduped = submit_era5_job(req)
    except RuntimeError as exc:
        raise ApiError(status_code=429, error_code="RATE_LIMIT", message=str(exc)) from exc

    if not deduped:
        kick_queued_jobs()

    return Era5IngestResponse(
        status="accepted",
        job_id=job_id,
        deduplicated=deduped,
        request_signature=sig,
    )


@router.post("/firms/ingest", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def firms_ingest(
    body: dict,
    _: str = Depends(verify_token),
):
    # Contract defines inline schema; we parse and validate required fields here.
    try:
        source = str(body["source"])
        bbox = BBox(**body["bbox"]).model_dump()
        start_date = date.fromisoformat(body["start_date"])
        end_date = date.fromisoformat(body["end_date"])
    except Exception as exc:  # noqa: BLE001
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message="Invalid FIRMS ingest payload") from exc
    if start_date > end_date:
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message="start_date must be <= end_date")
    return create_firms_ingest_job(
        source=source,
        bbox=bbox,
        start_date=start_date,
        end_date=end_date,
    )


@router.post("/glofas/backfill", response_model=JobStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def glofas_backfill(body: GlofasBackfillRequest, _: str = Depends(verify_token)):
    return create_glofas_backfill_job(
        start=body.start,
        end=body.end,
        concurrency=body.concurrency,
    )


@router.get("/glofas/status", response_model=GlofasStatusResponse)
async def glofas_status(_: str = Depends(verify_token)):
    return get_glofas_status_payload()


@router.get("/{job_id}", response_model=JobStatusResponse, summary="Get asynchronous ERA5/FIRMS job status")
async def job_status(job_id: str, _: str = Depends(verify_token)):
    return get_job_status_payload(job_id)


@router.get(
    "/climate/series",
    response_model=ClimateSeriesResponse,
    summary="Unified climate time series (history from ERA5, current from Open-Meteo)",
)
async def climate_series(
    lat: float = Query(..., ge=35.0, le=42.8),
    lng: float = Query(..., ge=25.0, le=45.5),
    start_date: date = Query(...),
    end_date: date = Query(...),
    _: str = Depends(verify_token),
):
    # Legacy endpoint kept for compatibility.
    if start_date > end_date:
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message="start_date must be <= end_date")

    era5 = {r["date"]: r for r in get_era5_features(lat, lng, start_date, end_date)}
    open_meteo = {r["date"]: r for r in fetch_open_meteo_daily(lat, lng, start_date, end_date)}
    today = fetch_open_meteo_today(lat, lng)
    if today:
        open_meteo[today["date"]] = today

    points: list[ClimatePoint] = []
    today_date = date.today()
    cur = start_date
    while cur <= end_date:
        key = cur.isoformat()
        if cur < today_date:
            row = era5.get(key)
        else:
            row = open_meteo.get(key) or era5.get(key)
        if row:
            points.append(
                ClimatePoint(
                    date=cur,
                    temp_mean=row.get("temp_mean"),
                    temp_max=row.get("temp_max"),
                    precip_sum=row.get("precip_sum"),
                    wind_max=row.get("wind_max"),
                    soil_moisture_mean=row.get("soil_moisture_mean"),
                    source=row.get("source", "unknown"),
                )
            )
        cur = cur + timedelta(days=1)

    source = "era5+open-meteo" if any(p.source == "era5" for p in points) else "open-meteo"
    return ClimateSeriesResponse(
        data=points,
        data_source=source,
        as_of_date=end_date,
    )
