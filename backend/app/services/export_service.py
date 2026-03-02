from __future__ import annotations

from datetime import timedelta
import csv
import io
import os
from typing import Any
from uuid import uuid4

import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import storage
from sqlalchemy import select

from app.config import settings
from app.database import SessionLocal
from app.errors import ApiError
from app.orm import ExportJobORM, PortfolioAssetORM
from pipeline.era5_ingestion import save_export_job
from pipeline.risk_scoring import save_portfolio_assets
from app.services.scoring_service import normalize_perils
from pipeline.risk_scoring import batch_score_assets


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


def create_portfolio_export(*, portfolio_id: str, start_date, end_date, include_drivers: bool) -> dict[str, Any]:
    if not settings.era5_gcs_bucket:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message="ERA5_GCS_BUCKET is missing")

    export_id = str(uuid4())
    with SessionLocal() as db:
        rows = db.execute(select(PortfolioAssetORM).where(PortfolioAssetORM.portfolio_id == portfolio_id)).scalars().all()
    assets_payload = [{"asset_id": r.asset_id, "lat": r.lat, "lon": r.lon} for r in rows]
    if not assets_payload:
        raise ApiError(status_code=404, error_code="NOT_FOUND", message=f"Portfolio '{portfolio_id}' not found")

    save_portfolio_assets(portfolio_id, assets_payload)
    scored = batch_score_assets(
        assets=assets_payload,
        start_date=start_date,
        end_date=end_date,
        climatology_version="v1_baseline_2015_2024",
        persist=True,
        include_perils=normalize_perils(["heat", "rain", "wind", "drought"]),
    )
    out_rows: list[dict[str, Any]] = []
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
        if include_drivers:
            for p in ["heat", "rain", "wind", "drought", "wildfire"]:
                if p in by_peril and by_peril[p].get("drivers"):
                    top_drivers.append(f"{p}:{by_peril[p]['drivers'][0]}")
        out_rows.append(
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
    for row in out_rows:
        writer.writerow(row)
    csv_bytes = buffer.getvalue().encode("utf-8")
    object_name = f"exports/{portfolio_id}/{export_id}.csv"
    client = storage.Client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    signed_url: str | None = _generate_signed_url(blob, expiration_hours=6)
    gcs_uri = f"gs://{settings.era5_gcs_bucket}/{object_name}"

    save_export_job(
        ExportJobORM(
            export_id=export_id,
            portfolio_id=portfolio_id,
            scenario="historical",
            start_date=start_date,
            end_date=end_date,
            output_format="csv",
            status="success",
            row_count=len(out_rows),
            gcs_uri=gcs_uri,
            signed_url=signed_url,
            error=None,
        )
    )

    return {"export_id": export_id, "status": "success", "path": gcs_uri, "download_url": signed_url}

