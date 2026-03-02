from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import csv
import io
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from google.cloud import storage

from app.auth import verify_token
from app.config import settings
from app.era5_presets import CORE_VARIABLES, FULL_VARIABLES
from app.models import (
    ClimatologyBuildRequest,
    ClimatologyBuildResponse,
    Era5BackfillRequest,
    Era5BackfillResponse,
    Era5BackfillStatusResponse,
    Era5BatchFeatureRequest,
    PortfolioExportRequest,
    PortfolioExportResponse,
    PortfolioRiskSummaryResponse,
    ScoreBatchRequest,
    ScoreBatchResponse,
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
from pipeline.risk_scoring import (
    batch_score_assets,
    build_climatology,
    portfolio_risk_summary,
    save_portfolio_assets,
)

router = APIRouter()


def _verify_cron_secret(x_cron_secret: str | None) -> None:
    if not settings.cron_secret:
        raise HTTPException(status_code=503, detail="CRON_SECRET is not configured")
    if x_cron_secret != settings.cron_secret:
        raise HTTPException(status_code=401, detail="Invalid cron secret")


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
        concurrency=body.concurrency,
    )
    return Era5BackfillResponse(status="accepted", backfill_id=backfill_id, deduplicated=dedup, months_total=months_total)


@router.get("/jobs/era5/variable-profiles")
async def variable_profiles(_: str = Depends(verify_token)):
    return {
        "status": "success",
        "profiles": {
            "core": {"count": len(CORE_VARIABLES), "variables": CORE_VARIABLES},
            "full": {"count": len(FULL_VARIABLES), "variables": FULL_VARIABLES},
        },
    }


@router.get("/jobs/era5/backfill/{backfill_id}", response_model=Era5BackfillStatusResponse)
async def backfill_status(backfill_id: str, _: str = Depends(verify_token)):
    from pipeline.era5_ingestion import get_backfill_status

    status = get_backfill_status(backfill_id, include_items=True)
    if not status:
        raise HTTPException(status_code=404, detail=f"Backfill '{backfill_id}' not found")
    kick_queued_jobs()
    return Era5BackfillStatusResponse(**status)


@router.post("/climatology/build", response_model=ClimatologyBuildResponse)
async def climatology_build(body: ClimatologyBuildRequest, _: str = Depends(verify_token)):
    out = build_climatology(
        baseline_start=body.baseline_start,
        baseline_end=body.baseline_end,
        climatology_version=body.climatology_version,
        level=body.level,
    )
    return ClimatologyBuildResponse(status="success", **out)


@router.post("/scores/batch", response_model=ScoreBatchResponse)
async def scores_batch(body: ScoreBatchRequest, _: str = Depends(verify_token)):
    assets = [{"asset_id": a.id, "lat": a.lat, "lon": a.lon} for a in body.assets]
    out = batch_score_assets(
        assets=assets,
        start_date=body.start_date,
        end_date=body.end_date,
        climatology_version=body.climatology_version,
        persist=body.persist,
    )
    typed_assets: dict[str, list] = {}
    for aid, rows in out["assets"].items():
        typed_assets[aid] = rows
    return ScoreBatchResponse(
        status="success",
        run_id=out["run_id"],
        climatology_version=body.climatology_version,
        assets=typed_assets,
    )


@router.get("/portfolios/{portfolio_id}/risk-summary", response_model=PortfolioRiskSummaryResponse)
async def portfolio_summary(
    portfolio_id: str,
    start: date = Query(...),
    end: date = Query(...),
    _: str = Depends(verify_token),
):
    out = portfolio_risk_summary(portfolio_id, start, end)
    return PortfolioRiskSummaryResponse(
        status="success",
        portfolio_id=portfolio_id,
        start_date=start,
        end_date=end,
        distribution=out["distribution"],
        top_10_assets=out["top_10_assets"],
        trend_summary=out["trend_summary"],
    )


@router.post("/cron/era5/daily-update")
async def era5_daily_update(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)
    target = datetime.now(timezone.utc).date() - timedelta(days=3)
    req = Era5Request(
        start_date=target,
        end_date=target,
        bbox=(42.0, 26.0, 36.0, 45.0),
        variables=CORE_VARIABLES,
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
    if body.assets:
        save_portfolio_assets(
            body.portfolio_id,
            [{"asset_id": a.id, "lat": a.lat, "lon": a.lon} for a in body.assets],
        )
    assets_payload = [{"asset_id": a.id, "lat": a.lat, "lon": a.lon} for a in body.assets]
    scored = batch_score_assets(
        assets=assets_payload,
        start_date=body.start_date,
        end_date=body.end_date,
        climatology_version=body.climatology_version,
        persist=True,
    )
    rows: list[dict] = []
    for a in body.assets:
        entries = scored["assets"].get(a.id, [])
        if not entries:
            continue
        by_peril: dict[str, dict] = {}
        for e in entries:
            p = e["peril"]
            if p not in by_peril or e["score_0_100"] > by_peril[p]["score_0_100"]:
                by_peril[p] = e
        top_drivers = []
        if body.include_drivers:
            for p in ["heat", "rain", "wind", "drought"]:
                if p in by_peril and by_peril[p].get("drivers"):
                    top_drivers.append(f"{p}:{by_peril[p]['drivers'][0]}")
        rows.append(
            {
                "asset_id": a.id,
                "lat": a.lat,
                "lon": a.lon,
                "score_heat": by_peril.get("heat", {}).get("score_0_100", 0),
                "score_precip": by_peril.get("rain", {}).get("score_0_100", 0),
                "score_wind": by_peril.get("wind", {}).get("score_0_100", 0),
                "score_drought": by_peril.get("drought", {}).get("score_0_100", 0),
                "top_drivers": " | ".join(top_drivers),
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
    return PortfolioExportResponse(
        status="success",
        export_id=export_id,
        row_count=len(rows),
        export_url=signed_url or gcs_uri,
    )


@router.get("/health/metrics")
async def metrics(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)
    return {"status": "success", **get_jobs_metrics(24)}
