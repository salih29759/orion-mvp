from __future__ import annotations

from datetime import date, timedelta
import json

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.auth import verify_token
from app.models import (
    ClimatePoint,
    ClimateSeriesResponse,
    Era5IngestRequest,
    Era5IngestResponse,
    JobStatusResponse,
)
from pipeline.era5_ingestion import (
    Era5Request,
    get_backfill_status,
    get_era5_features,
    get_job,
    kick_queued_jobs,
    request_signature,
    submit_era5_job,
    validate_era5_runtime,
)
from pipeline.firms_ingestion import get_firms_job
from pipeline.open_meteo_series import fetch_open_meteo_daily, fetch_open_meteo_today

router = APIRouter()


def _to_job_response(job) -> JobStatusResponse:
    dq_report = None
    if job.dq_report_json:
        try:
            dq_report = json.loads(job.dq_report_json)
        except Exception:  # noqa: BLE001
            dq_report = None
    return JobStatusResponse(
        status=job.status,
        job_id=job.job_id,
        request_signature=job.request_signature,
        dataset=job.dataset,
        variables=[x.strip() for x in job.variables_csv.split(",") if x.strip()],
        bbox=[float(x) for x in job.bbox_csv.split(",")],
        start_date=job.start_date,
        end_date=job.end_date,
        rows_written=job.rows_written,
        bytes_downloaded=job.bytes_downloaded,
        raw_files=job.raw_files,
        feature_files=job.feature_files,
        dq_status=job.dq_status,
        dq_report=dq_report,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error,
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
    missing = validate_era5_runtime()
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"ERA5 ingestion is not configured. Missing env vars: {', '.join(missing)}",
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
        raise HTTPException(status_code=429, detail=str(exc)) from exc

    if not deduped:
        kick_queued_jobs()

    return Era5IngestResponse(
        status="accepted",
        job_id=job_id,
        deduplicated=deduped,
        request_signature=sig,
    )


@router.get("/{job_id}", summary="Get asynchronous ERA5 ingestion/backfill job status")
async def job_status(job_id: str, _: str = Depends(verify_token)):
    # Backfill status (with child jobs)
    bf = get_backfill_status(job_id, include_items=True)
    if bf:
        status = "success" if bf.get("status") == "finished" else bf.get("status")
        children = [c["job_id"] for c in (bf.get("child_jobs") or []) if c.get("job_id")]
        return {
            "job_id": bf["backfill_id"],
            "status": status,
            "type": "era5_backfill",
            "created_at": bf.get("created_at"),
            "updated_at": bf.get("finished_at"),
            "progress": {
                "months_total": bf.get("months_total", 0),
                "months_success": bf.get("months_success", 0),
                "months_failed": bf.get("months_failed", 0),
                "failed_months": bf.get("failed_months", []),
            },
            "children": children,
            # backward-compatible fields
            **bf,
        }
    firms = get_firms_job(job_id)
    if firms:
        return {
            "job_id": firms.job_id,
            "status": firms.status,
            "type": "firms_ingest",
            "created_at": firms.created_at,
            "updated_at": firms.finished_at or firms.started_at,
            "progress": {
                "rows_fetched": firms.rows_fetched,
                "rows_inserted": firms.rows_inserted,
                "raw_gcs_uri": firms.raw_gcs_uri,
            },
            "children": [],
            "error": firms.error,
            "source": firms.source,
            "start_date": firms.start_date,
            "end_date": firms.end_date,
        }
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    payload = _to_job_response(job).model_dump()
    payload.update(
        {
            "type": "era5_ingest",
            "updated_at": payload.get("finished_at") or payload.get("started_at"),
            "progress": {
                "rows_written": payload.get("rows_written", 0),
                "bytes_downloaded": payload.get("bytes_downloaded", 0),
                "dq_status": payload.get("dq_status"),
            },
            "children": [],
        }
    )
    return payload


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
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

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
