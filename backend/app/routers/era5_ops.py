from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import csv
import io
import json
import os
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query
import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import storage
from sqlalchemy import select

from app.auth import verify_token
from app.config import settings
from app.database import SessionLocal
from app.era5_presets import CORE_VARIABLES, FULL_VARIABLES
from app.models import (
    AckNotificationResponse,
    ClimatologyBuildRequest,
    ClimatologyBuildResponse,
    Era5BackfillRequest,
    Era5BackfillStatusResponse,
    Era5BatchFeatureRequest,
    FirmsIngestRequest,
    FirmsIngestResponse,
    NotificationItem,
    PortfolioItem,
    PortfolioExportRequest,
    PortfolioExportResponse,
    PortfolioRiskSummaryResponse,
    ScoreBatchRequest,
    ScoreBatchResponse,
    ScoreBenchmarkRequest,
    ScoreBenchmarkResponse,
    WildfireFeaturesResponse,
)
from app.orm import ExportJobORM, PortfolioAssetORM
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
from pipeline.firms_ingestion import (
    ack_notification,
    get_asset_wildfire_features,
    get_firms_metrics,
    list_notifications,
    run_daily_firms_update,
    submit_firms_ingest,
    FirmsRequest,
)
from pipeline.risk_scoring import (
    batch_score_assets,
    benchmark_batch_scoring,
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


def _generate_signed_url(blob, expiration_hours: int = 6) -> str | None:
    try:
        return blob.generate_signed_url(version="v4", expiration=timedelta(hours=expiration_hours), method="GET")
    except Exception:  # noqa: BLE001
        pass

    try:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(GoogleAuthRequest())
        sa_email = getattr(creds, "service_account_email", None) or os.getenv("K_SERVICE_ACCOUNT")
        if not sa_email:
            return None
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(hours=expiration_hours),
            method="GET",
            service_account_email=sa_email,
            access_token=creds.token,
        )
    except Exception:  # noqa: BLE001
        return None


def _normalize_perils(perils: list[str] | None) -> list[str]:
    allowed = {"heat", "rain", "wind", "drought", "wildfire"}
    if not perils:
        return ["heat", "rain", "wind", "drought"]
    if "all" in perils:
        return ["heat", "rain", "wind", "drought", "wildfire"]
    out = [p for p in perils if p in allowed]
    if not out:
        return ["heat", "rain", "wind", "drought"]
    return out


def _to_batch_results(assets_payload: dict[str, list[dict[str, Any]]], include_perils: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for asset_id, rows in assets_payload.items():
        by_date: dict[str, dict[str, Any]] = defaultdict(lambda: {"scores": {}, "bands": {}, "drivers": {}})
        for row in rows:
            dt = row["date"]
            peril = row["peril"]
            by_date[dt]["scores"][peril] = row["score_0_100"]
            by_date[dt]["bands"][peril] = row["band"].lower()
            by_date[dt]["drivers"][peril] = row.get("drivers", [])
        series: list[dict[str, Any]] = []
        for dt in sorted(by_date.keys()):
            point = by_date[dt]
            if include_perils:
                values = [point["scores"][p] for p in include_perils if p in point["scores"]]
                if values:
                    point["scores"]["all"] = round(sum(values) / len(values), 2)
                    point["bands"]["all"] = (
                        "extreme"
                        if point["scores"]["all"] >= 80
                        else "major"
                        if point["scores"]["all"] >= 60
                        else "moderate"
                        if point["scores"]["all"] >= 40
                        else "minor"
                        if point["scores"]["all"] >= 20
                        else "minimal"
                    )
            series.append({"date": dt, "scores": point["scores"], "bands": point["bands"], "drivers": point["drivers"]})
        results.append({"asset_id": asset_id, "series": series})
    return results


@router.post("/jobs/era5/backfill", status_code=202)
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
    now = datetime.now(timezone.utc)
    return {
        "status": "queued",
        "request_status": "accepted",
        "job_id": backfill_id,
        "backfill_id": backfill_id,
        "type": "era5_backfill",
        "created_at": now.isoformat(),
        "updated_at": None,
        "progress": {"months_total": months_total, "months_success": 0, "months_failed": 0},
        "children": [],
        "deduplicated": dedup,
        "months_total": months_total,
    }


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
    return ClimatologyBuildResponse(
        version=out["climatology_version"],
        status="success",
        row_count=out["row_count"],
    )


@router.post("/scores/batch", response_model=ScoreBatchResponse)
async def scores_batch(body: ScoreBatchRequest, _: str = Depends(verify_token)):
    include_perils = _normalize_perils(body.include_perils)
    assets = [{"asset_id": a.asset_id, "lat": a.lat, "lon": a.lon} for a in body.assets]
    out = batch_score_assets(
        assets=assets,
        start_date=body.start_date,
        end_date=body.end_date,
        climatology_version=body.climatology_version,
        persist=body.persist,
        include_perils=include_perils,
    )
    results = _to_batch_results(out["assets"], include_perils=include_perils)
    return ScoreBatchResponse(
        run_id=out["run_id"],
        climatology_version=body.climatology_version,
        results=results,
    )


@router.post("/scores/benchmark", response_model=ScoreBenchmarkResponse)
async def scores_benchmark(body: ScoreBenchmarkRequest, _: str = Depends(verify_token)):
    out = benchmark_batch_scoring(
        assets_count=body.assets_count,
        start_date=body.start_date,
        end_date=body.end_date,
        climatology_version=body.climatology_version,
    )
    return ScoreBenchmarkResponse(status="success", **out)


@router.get("/portfolios", response_model=list[PortfolioItem])
async def list_portfolios(_: str = Depends(verify_token)):
    with SessionLocal() as db:
        ids = db.execute(select(PortfolioAssetORM.portfolio_id).distinct().order_by(PortfolioAssetORM.portfolio_id)).all()
    return [{"portfolio_id": r[0], "name": r[0]} for r in ids]


@router.get("/portfolios/{portfolio_id}/risk-summary", response_model=PortfolioRiskSummaryResponse)
async def portfolio_summary(
    portfolio_id: str,
    start: date = Query(...),
    end: date = Query(...),
    _: str = Depends(verify_token),
):
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")
    out = portfolio_risk_summary(portfolio_id, start, end)
    period = {"start": start, "end": end}
    return PortfolioRiskSummaryResponse(
        portfolio_id=portfolio_id,
        period=period,
        bands=out.get("bands", {}),
        peril_averages=out.get("peril_averages", {}),
        top_assets=out.get("top_assets", []),
        trend=out.get("trend", []),
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


@router.post("/jobs/firms/ingest", response_model=FirmsIngestResponse, status_code=202)
async def create_firms_ingest(body: FirmsIngestRequest, _: str = Depends(verify_token)):
    req = FirmsRequest(
        source=body.source,
        bbox=(body.bbox["north"], body.bbox["west"], body.bbox["south"], body.bbox["east"]),
        start_date=body.start_date,
        end_date=body.end_date,
    )
    job_id, dedup = submit_firms_ingest(req)
    return FirmsIngestResponse(
        status="queued",
        request_status="accepted",
        job_id=job_id,
        type="firms_ingest",
        created_at=datetime.now(timezone.utc),
        deduplicated=dedup,
    )


@router.post("/cron/firms/daily-update")
async def firms_daily_update(x_cron_secret: str | None = Header(default=None)):
    _verify_cron_secret(x_cron_secret)
    job_id, dedup, start_date, end_date = run_daily_firms_update()
    return {
        "status": "accepted",
        "job_id": job_id,
        "deduplicated": dedup,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


@router.get("/assets/{asset_id}/wildfire-features", response_model=WildfireFeaturesResponse)
async def wildfire_features(asset_id: str, window: str = Query("24h", pattern="^(24h|7d)$"), _: str = Depends(verify_token)):
    features = get_asset_wildfire_features(asset_id, window)
    if features is None:
        raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' not found")
    return WildfireFeaturesResponse(status="success", asset_id=asset_id, window=window, **features)


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
        out[asset.asset_id] = get_era5_features(asset.lat, asset.lon, body.start_date, body.end_date)
    return {"status": "success", "assets": out}


@router.post("/export/portfolio", response_model=PortfolioExportResponse)
async def export_portfolio(body: PortfolioExportRequest, _: str = Depends(verify_token)):
    if not settings.era5_gcs_bucket:
        raise HTTPException(status_code=503, detail="ERA5_GCS_BUCKET is missing")
    export_id = str(uuid4())
    assets_payload = [{"asset_id": a.asset_id, "lat": a.lat, "lon": a.lon} for a in body.assets]
    if assets_payload:
        save_portfolio_assets(body.portfolio_id, assets_payload)
    else:
        with SessionLocal() as db:
            rows = db.execute(select(PortfolioAssetORM).where(PortfolioAssetORM.portfolio_id == body.portfolio_id)).scalars().all()
        assets_payload = [{"asset_id": r.asset_id, "lat": r.lat, "lon": r.lon} for r in rows]
    scored = batch_score_assets(
        assets=assets_payload,
        start_date=body.start_date,
        end_date=body.end_date,
        climatology_version=body.climatology_version,
        persist=True,
        include_perils=_normalize_perils(["all"] if body.include_wildfire else ["heat", "rain", "wind", "drought"]),
    )
    rows: list[dict[str, Any]] = []
    for a in assets_payload:
        entries = scored["assets"].get(a["asset_id"], [])
        if not entries:
            continue
        by_peril: dict[str, dict[str, Any]] = {}
        for e in entries:
            p = e["peril"]
            if p not in by_peril or e["score_0_100"] > by_peril[p]["score_0_100"]:
                by_peril[p] = e
        top_drivers = []
        if body.include_drivers:
            for p in ["heat", "rain", "wind", "drought", "wildfire"]:
                if p in by_peril and by_peril[p].get("drivers"):
                    top_drivers.append(f"{p}:{by_peril[p]['drivers'][0]}")
        rows.append(
            {
                "asset_id": a["asset_id"],
                "lat": a["lat"],
                "lon": a["lon"],
                "score_heat": by_peril.get("heat", {}).get("score_0_100", 0),
                "score_precip": by_peril.get("rain", {}).get("score_0_100", 0),
                "score_wind": by_peril.get("wind", {}).get("score_0_100", 0),
                "score_drought": by_peril.get("drought", {}).get("score_0_100", 0),
                "score_wildfire": by_peril.get("wildfire", {}).get("score_0_100", 0),
                "top_drivers": " | ".join(top_drivers),
            }
        )

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "asset_id",
            "lat",
            "lon",
            "score_heat",
            "score_precip",
            "score_wind",
            "score_drought",
            "score_wildfire",
            "top_drivers",
        ],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    csv_bytes = buffer.getvalue().encode("utf-8")
    object_name = f"exports/{body.portfolio_id}/{export_id}.csv"
    client = storage.Client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    signed_url: str | None = _generate_signed_url(blob, expiration_hours=6)
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
        export_id=export_id,
        status="success",
        path=gcs_uri,
        download_url=signed_url,
    )


@router.get("/notifications", response_model=list[NotificationItem])
async def notifications(portfolio_id: str | None = Query(default=None), _: str = Depends(verify_token)):
    rows = list_notifications(portfolio_id=portfolio_id)
    out: list[NotificationItem] = []
    for row in rows:
        payload = {}
        if row.payload_json:
            try:
                payload = json.loads(row.payload_json)
            except Exception:  # noqa: BLE001
                payload = {"raw": row.payload_json}
        severity = (row.severity or "low").lower()
        if severity not in {"low", "medium", "high"}:
            severity = "low"
        out.append(
            NotificationItem(
                id=row.id,
                severity=severity,
                type=row.type,
                portfolio_id=row.portfolio_id,
                asset_id=row.asset_id,
                created_at=row.created_at,
                acknowledged_at=row.acknowledged_at,
                payload=payload,
            )
        )
    return out


@router.post("/notifications/{notification_id}/ack", response_model=AckNotificationResponse)
async def notification_ack(notification_id: str, _: str = Depends(verify_token)):
    row = ack_notification(notification_id)
    if not row or not row.get("acknowledged_at"):
        raise HTTPException(status_code=404, detail=f"Notification '{notification_id}' not found")
    return AckNotificationResponse(id=row["id"], acknowledged_at=row["acknowledged_at"])


@router.get("/health/metrics")
async def metrics(x_cron_secret: str | None = Header(default=None)):
    # Backward-compatible: keep cron secret auth path, but allow no header.
    if x_cron_secret is not None:
        _verify_cron_secret(x_cron_secret)
    era5 = get_jobs_metrics(24)
    firms = get_firms_metrics(24)
    return {
        "status": "success",
        "jobs_last_24h": era5["jobs_last_24h"],
        "success_rate": era5["success_rate"],
        "avg_duration_seconds": era5["avg_duration"],
        "bytes_downloaded_last_24h": era5["bytes_downloaded"],
        # backward-compatible fields
        "avg_duration": era5["avg_duration"],
        "bytes_downloaded": era5["bytes_downloaded"],
        **firms,
    }
