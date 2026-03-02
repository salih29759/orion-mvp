from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import csv
import io
import json
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from google.cloud import storage
import pandas as pd

from app.auth import verify_token
from app.config import settings
from app.models import (
    Era5BackfillRequest,
    Era5BackfillResponse,
    Era5BackfillStatusResponse,
    Era5BatchFeatureRequest,
    PortfolioExportRequest,
    PortfolioExportResponse,
)
from app.orm import ExportJobORM
from pipeline.era5_ingestion import (
    Era5Request,
    get_era5_features,
    get_jobs_metrics,
    kick_queued_jobs,
    save_export_job,
    submit_backfill,
    submit_era5_job,
    validate_era5_runtime,
)

router = APIRouter()


def _verify_cron_secret(x_cron_secret: str | None) -> None:
    if not settings.cron_secret:
        raise HTTPException(status_code=503, detail="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise HTTPException(status_code=401, detail="Invalid cron secret")


def _score(v: float | None, low: float, high: float) -> int:
    if v is None:
        return 0
    if v <= low:
        return 0
    if v >= high:
        return 100
    return int(((v - low) / (high - low)) * 100)


@router.post("/jobs/era5/backfill", response_model=Era5BackfillResponse, status_code=202)
async def create_backfill(body: Era5BackfillRequest, _: str = Depends(verify_token)):
    missing = validate_era5_runtime()
    if missing:
        raise HTTPException(status_code=503, detail=f"Missing env vars: {', '.join(missing)}")

    bbox = (body.bbox["north"], body.bbox["west"], body.bbox["south"], body.bbox["east"])
    backfill_id, dedup, months_total = submit_backfill(
        start_month=body.start_month,
        end_month=body.end_month,
        bbox=bbox,
        variables=body.variables,
        mode=body.mode,
        dataset=body.dataset,
    )
    return Era5BackfillResponse(status="accepted", backfill_id=backfill_id, deduplicated=dedup, months_total=months_total)


@router.get("/jobs/era5/backfill/{backfill_id}", response_model=Era5BackfillStatusResponse)
async def backfill_status(backfill_id: str, _: str = Depends(verify_token)):
    from pipeline.era5_ingestion import get_backfill_status

    status = get_backfill_status(backfill_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Backfill '{backfill_id}' not found")
    kick_queued_jobs()
    return Era5BackfillStatusResponse(**status)


@router.post("/cron/era5/daily-update")
async def era5_daily_update(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)
    target = datetime.now(timezone.utc).date() - timedelta(days=3)
    req = Era5Request(
        start_date=target,
        end_date=target,
        bbox=(42.0, 26.0, 36.0, 45.0),
        variables=[
            "2m_temperature",
            "total_precipitation",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "volumetric_soil_water_layer_1",
        ],
        dataset="era5-land",
        out_format="netcdf",
    )
    job_id, dedup = submit_era5_job(req, enforce_limit=False)
    if not dedup:
        kick_queued_jobs()
    return {"status": "accepted", "target_date": target.isoformat(), "job_id": job_id, "deduplicated": dedup}


@router.get("/features/era5")
async def era5_features(
    lat: float = Query(...),
    lon: float = Query(...),
    start: date = Query(...),
    end: date = Query(...),
    _: str = Depends(verify_token),
):
    data = get_era5_features(lat, lon, start, end)
    return {"status": "success", "count": len(data), "data": data}


@router.post("/features/era5/batch")
async def era5_features_batch(body: Era5BatchFeatureRequest, _: str = Depends(verify_token)):
    out: dict[str, list[dict]] = {}
    for asset in body.assets:
        out[asset.id] = get_era5_features(asset.lat, asset.lon, body.start_date, body.end_date)
    return {"status": "success", "assets": out}


@router.post("/export/portfolio", response_model=PortfolioExportResponse)
async def export_portfolio(body: PortfolioExportRequest, _: str = Depends(verify_token)):
    if not settings.era5_gcs_bucket:
        raise HTTPException(status_code=503, detail="ERA5_GCS_BUCKET is missing")
    export_id = str(uuid4())
    rows: list[dict] = []
    for asset in body.assets:
        series = get_era5_features(asset.lat, asset.lon, body.start_date, body.end_date)
        if not series:
            continue
        df = pd.DataFrame(series)
        score_heat = _score(float(df["temp_max"].max()) if "temp_max" in df else None, 15, 40)
        score_precip = _score(float(df["precip_sum"].max()) if "precip_sum" in df else None, 10, 120)
        score_wind = _score(float(df["wind_max"].max()) if "wind_max" in df else None, 4, 20)
        score_drought = _score(float(df["soil_moisture_mean"].min()) if "soil_moisture_mean" in df else None, 0.1, 0.35)
        rows.append(
            {
                "asset_id": asset.id,
                "lat": asset.lat,
                "lon": asset.lon,
                "score_heat": score_heat,
                "score_precip": score_precip,
                "score_wind": score_wind,
                "score_drought": 100 - score_drought,
                "top_drivers": "temp_max,precip_sum,wind_max,soil_moisture_mean",
            }
        )

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["asset_id", "lat", "lon", "score_heat", "score_precip", "score_wind", "score_drought", "top_drivers"])
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    csv_bytes = buffer.getvalue().encode("utf-8")
    object_name = f"exports/{body.portfolio_id}/{export_id}.csv"
    client = storage.Client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    signed_url: str | None
    try:
        signed_url = blob.generate_signed_url(version="v4", expiration=timedelta(hours=6), method="GET")
    except Exception:  # noqa: BLE001
        # Cloud Run default compute credentials may not have a signing key.
        signed_url = None
    gcs_uri = f"gs://{settings.era5_gcs_bucket}/{object_name}"

    save_export_job(
        ExportJobORM(
            export_id=export_id,
            portfolio_id=body.portfolio_id,
            scenario=body.scenario,
            start_date=body.start_date,
            end_date=body.end_date,
            output_format=body.format,
            status="success",
            row_count=len(rows),
            gcs_uri=gcs_uri,
            signed_url=signed_url,
            error=None,
        )
    )
    return PortfolioExportResponse(status="success", export_id=export_id, row_count=len(rows), export_url=signed_url)


@router.get("/health/metrics")
async def metrics(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)
    return {"status": "success", **get_jobs_metrics(24)}
