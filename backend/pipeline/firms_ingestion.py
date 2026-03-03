from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import csv
import io
import json
import logging
import math
import threading
from typing import Any
from uuid import uuid4

from google.cloud import storage
import pandas as pd
import requests
from sqlalchemy import and_, desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import FireEventORM, FirmsIngestJobORM, NotificationORM, PortfolioAssetORM

LOG = logging.getLogger("orion.firms")
FIRMS_AREA_CSV_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"


@dataclass
class FirmsRequest:
    source: str
    bbox: tuple[float, float, float, float]
    start_date: date
    end_date: date


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(1e-12, 1 - a)))
    return r * c


def _bbox_to_csv(bbox: tuple[float, float, float, float]) -> str:
    north, west, south, east = bbox
    return f"{west},{south},{east},{north}"


def _parse_time_utc(row: dict[str, Any]) -> datetime | None:
    acq_date = str(row.get("acq_date", "")).strip()
    acq_time = str(row.get("acq_time", "")).strip()
    if acq_date:
        if acq_time:
            hhmm = acq_time.zfill(4)
            try:
                return datetime.strptime(f"{acq_date} {hhmm}", "%Y-%m-%d %H%M").replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        try:
            return datetime.strptime(acq_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    for key in ("time_utc", "datetime", "acq_datetime"):
        raw = row.get(key)
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _normalize_fire_rows(req: FirmsRequest, csv_text: str) -> list[dict[str, Any]]:
    north, west, south, east = req.bbox
    out: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        try:
            lat = float(row.get("latitude", row.get("lat")))
            lon = float(row.get("longitude", row.get("lon")))
        except (TypeError, ValueError):
            continue
        if lat < south or lat > north or lon < west or lon > east:
            continue
        dt = _parse_time_utc(row)
        if not dt:
            continue
        if dt.date() < req.start_date or dt.date() > req.end_date:
            continue
        frp_raw = row.get("frp")
        frp = None
        if frp_raw not in (None, ""):
            try:
                frp = float(frp_raw)
            except (TypeError, ValueError):
                frp = None
        confidence = row.get("confidence")
        satellite = row.get("satellite") or row.get("instrument")
        out.append(
            {
                "time_utc": dt,
                "lat": lat,
                "lon": lon,
                "lat_round": round(lat, 4),
                "lon_round": round(lon, 4),
                "geom_wkt": f"POINT({lon} {lat})",
                "frp": frp,
                "confidence": str(confidence) if confidence not in (None, "") else None,
                "satellite": str(satellite) if satellite not in (None, "") else None,
            }
        )
    return out


def _fetch_firms_csv(req: FirmsRequest) -> str:
    if not settings.firms_map_key:
        raise RuntimeError("FIRMS_MAP_KEY is missing")
    today = datetime.now(timezone.utc).date()
    if req.start_date > today:
        raise RuntimeError("start_date cannot be in the future")
    day_range = (today - req.start_date).days + 1
    # FIRMS area API is intended for near real-time windows; keep bounded.
    day_range = max(1, min(day_range, 10))
    bbox_csv = _bbox_to_csv(req.bbox)
    url = f"{FIRMS_AREA_CSV_URL}/{settings.firms_map_key}/{req.source}/{bbox_csv}/{day_range}"
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    return resp.text


def _upload_raw_csv(job_id: str, csv_text: str) -> str | None:
    if not settings.era5_gcs_bucket:
        return None
    now = datetime.now(timezone.utc)
    object_name = f"raw/firms/{now.year:04d}/{now.month:02d}/{now.day:02d}/{job_id}.csv"
    client = storage.Client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_string(csv_text, content_type="text/csv")
    return f"gs://{settings.era5_gcs_bucket}/{object_name}"


def _insert_fire_events(job_id: str, source: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    # Request-level dedupe before DB upsert.
    uniq: dict[tuple[str, float, float], dict[str, Any]] = {}
    for row in rows:
        key = (row["time_utc"].isoformat(), row["lat_round"], row["lon_round"])
        uniq[key] = row
    rows = list(uniq.values())
    min_ts = min(r["time_utc"] for r in rows)
    max_ts = max(r["time_utc"] for r in rows)
    inserted = 0
    with SessionLocal() as db:
        existing = db.execute(
            select(FireEventORM.time_utc, FireEventORM.lat_round, FireEventORM.lon_round).where(
                FireEventORM.source == source,
                FireEventORM.time_utc >= min_ts,
                FireEventORM.time_utc <= max_ts,
            )
        ).all()
        existing_keys = {(r[0].isoformat(), float(r[1]), float(r[2])) for r in existing}
        for row in rows:
            k = (row["time_utc"].isoformat(), row["lat_round"], row["lon_round"])
            if k in existing_keys:
                continue
            db.add(
                FireEventORM(
                    time_utc=row["time_utc"],
                    lat=row["lat"],
                    lon=row["lon"],
                    lat_round=row["lat_round"],
                    lon_round=row["lon_round"],
                    geom_wkt=row["geom_wkt"],
                    frp=row["frp"],
                    confidence=row["confidence"],
                    satellite=row["satellite"],
                    source=source,
                    raw_job_id=job_id,
                )
            )
            inserted += 1
        db.commit()
    return inserted


def load_fire_events_frame(start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    with SessionLocal() as db:
        rows = db.execute(
            select(FireEventORM).where(
                FireEventORM.time_utc >= start_ts,
                FireEventORM.time_utc <= end_ts,
            )
        ).scalars().all()
    if not rows:
        return pd.DataFrame(columns=["time_utc", "lat", "lon", "frp", "source"])
    return pd.DataFrame(
        [
            {
                "time_utc": r.time_utc,
                "lat": float(r.lat),
                "lon": float(r.lon),
                "frp": float(r.frp) if r.frp is not None else None,
                "source": r.source,
            }
            for r in rows
        ]
    )


def wildfire_features_for_point(
    *,
    lat: float,
    lon: float,
    events_df: pd.DataFrame,
    window_end: datetime,
) -> dict[str, Any]:
    if events_df.empty:
        return {
            "nearest_fire_distance_km_24h": None,
            "nearest_fire_distance_km_7d": None,
            "fires_within_10km_count_24h": 0,
            "fires_within_10km_count_7d": 0,
            "max_frp_within_20km_24h": None,
            "max_frp_within_20km_7d": None,
        }
    w24_start = window_end - timedelta(hours=24)
    w7_start = window_end - timedelta(days=7)
    df = events_df.copy()
    df["dist_km"] = df.apply(lambda r: _haversine_km(lat, lon, float(r["lat"]), float(r["lon"])), axis=1)
    sub24 = df[(df["time_utc"] >= w24_start) & (df["time_utc"] <= window_end)]
    sub7 = df[(df["time_utc"] >= w7_start) & (df["time_utc"] <= window_end)]

    def _nearest(sub: pd.DataFrame) -> float | None:
        if sub.empty:
            return None
        return round(float(sub["dist_km"].min()), 3)

    def _count10(sub: pd.DataFrame) -> int:
        if sub.empty:
            return 0
        return int((sub["dist_km"] <= 10.0).sum())

    def _max_frp20(sub: pd.DataFrame) -> float | None:
        if sub.empty:
            return None
        f = sub[sub["dist_km"] <= 20.0]["frp"].dropna()
        if f.empty:
            return None
        return round(float(f.max()), 3)

    return {
        "nearest_fire_distance_km_24h": _nearest(sub24),
        "nearest_fire_distance_km_7d": _nearest(sub7),
        "fires_within_10km_count_24h": _count10(sub24),
        "fires_within_10km_count_7d": _count10(sub7),
        "max_frp_within_20km_24h": _max_frp20(sub24),
        "max_frp_within_20km_7d": _max_frp20(sub7),
    }


def _generate_notifications() -> int:
    now = datetime.now(timezone.utc)
    fires = load_fire_events_frame(now - timedelta(days=7), now)
    if fires.empty:
        return 0
    with SessionLocal() as db:
        assets = db.execute(select(PortfolioAssetORM)).scalars().all()
        if not assets:
            return 0
        today = now.date().isoformat()
        inserted = 0
        existing = {
            row[0]
            for row in db.execute(
                select(NotificationORM.dedup_key).where(
                    and_(
                        NotificationORM.type == "wildfire_proximity",
                        NotificationORM.created_at >= now - timedelta(days=1),
                    )
                )
            ).all()
        }
        for a in assets:
            feat = wildfire_features_for_point(lat=float(a.lat), lon=float(a.lon), events_df=fires, window_end=now)
            nearest24 = feat["nearest_fire_distance_km_24h"]
            nearest7 = feat["nearest_fire_distance_km_7d"]
            count24 = int(feat["fires_within_10km_count_24h"])
            count7 = int(feat["fires_within_10km_count_7d"])
            severity = None
            if (nearest24 is not None and nearest24 < 5.0) or count24 >= 1:
                severity = "high"
            elif (nearest7 is not None and nearest7 < 15.0) or count7 >= 3:
                severity = "medium"
            if not severity:
                continue
            dedup_key = f"{a.portfolio_id}:{a.asset_id}:wildfire_proximity:{severity}:{today}"
            if dedup_key in existing:
                continue
            payload = {
                "nearest_fire_distance_km_24h": nearest24,
                "nearest_fire_distance_km_7d": nearest7,
                "fires_within_10km_count_24h": count24,
                "fires_within_10km_count_7d": count7,
                "max_frp_within_20km_24h": feat["max_frp_within_20km_24h"],
                "max_frp_within_20km_7d": feat["max_frp_within_20km_7d"],
            }
            db.add(
                NotificationORM(
                    id=f"ntf_{uuid4().hex[:20]}",
                    customer_id=None,
                    portfolio_id=a.portfolio_id,
                    asset_id=a.asset_id,
                    type="wildfire_proximity",
                    severity=severity,
                    payload_json=json.dumps(payload),
                    dedup_key=dedup_key,
                )
            )
            existing.add(dedup_key)
            inserted += 1
        db.commit()
    return inserted


def process_firms_job(job_id: str) -> None:
    t0 = datetime.now(timezone.utc)
    with SessionLocal() as db:
        job = db.get(FirmsIngestJobORM, job_id)
        if not job:
            return
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        db.commit()
        req = FirmsRequest(
            source=job.source,
            bbox=tuple(float(x) for x in job.bbox_csv.split(",")),  # type: ignore[arg-type]
            start_date=job.start_date,
            end_date=job.end_date,
        )
    try:
        raw_csv = _fetch_firms_csv(req)
        normalized = _normalize_fire_rows(req, raw_csv)
        raw_uri = _upload_raw_csv(job_id, raw_csv)
        inserted = _insert_fire_events(job_id, req.source, normalized)
        alerts_created = _generate_notifications()
        status = "success"
        error = None
    except Exception as exc:  # noqa: BLE001
        LOG.exception("firms job failed: %s", job_id)
        raw_uri = None
        normalized = []
        inserted = 0
        alerts_created = 0
        status = "failed"
        error = str(exc)
    with SessionLocal() as db:
        job = db.get(FirmsIngestJobORM, job_id)
        if not job:
            return
        job.status = status
        job.rows_fetched = len(normalized)
        job.rows_inserted = inserted
        job.raw_gcs_uri = raw_uri
        job.duration_seconds = (datetime.now(timezone.utc) - t0).total_seconds()
        job.error = error
        job.finished_at = datetime.now(timezone.utc)
        db.commit()
    LOG.info(
        json.dumps(
            {
                "event": "firms_job_finish",
                "job_id": job_id,
                "status": status,
                "rows_fetched": len(normalized),
                "rows_inserted": inserted,
                "alerts_generated": alerts_created,
                "duration_seconds": round((datetime.now(timezone.utc) - t0).total_seconds(), 3),
            }
        )
    )


def start_firms_background_job(job_id: str) -> None:
    t = threading.Thread(target=process_firms_job, args=(job_id,), daemon=False)
    t.start()


def submit_firms_ingest(req: FirmsRequest, *, start_async: bool = True) -> tuple[str, bool]:
    signature = sha256(
        json.dumps(
            {
                "source": req.source,
                "bbox": [round(x, 4) for x in req.bbox],
                "start_date": req.start_date.isoformat(),
                "end_date": req.end_date.isoformat(),
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    with SessionLocal() as db:
        existing = db.execute(
            select(FirmsIngestJobORM).where(FirmsIngestJobORM.request_signature == signature).limit(1)
        ).scalar_one_or_none()
        if existing:
            if existing.status in {"queued", "running", "success"}:
                return existing.job_id, True
            existing.status = "queued"
            existing.rows_fetched = 0
            existing.rows_inserted = 0
            existing.raw_gcs_uri = None
            existing.duration_seconds = None
            existing.error = None
            existing.started_at = None
            existing.finished_at = None
            db.commit()
            job_id = existing.job_id
            if start_async:
                start_firms_background_job(job_id)
            return job_id, False
        job_id = str(uuid4())
        db.add(
            FirmsIngestJobORM(
                job_id=job_id,
                request_signature=signature,
                status="queued",
                source=req.source,
                bbox_csv=",".join(str(x) for x in req.bbox),
                start_date=req.start_date,
                end_date=req.end_date,
                rows_fetched=0,
                rows_inserted=0,
            )
        )
        db.commit()
    if start_async:
        start_firms_background_job(job_id)
    return job_id, False


def get_firms_job(job_id: str) -> FirmsIngestJobORM | None:
    with SessionLocal() as db:
        return db.get(FirmsIngestJobORM, job_id)


def run_daily_firms_update() -> tuple[str, bool, date, date]:
    today = datetime.now(timezone.utc).date()
    days = max(1, int(settings.firms_day_range))
    start = today - timedelta(days=days - 1)
    req = FirmsRequest(
        source=settings.firms_source,
        bbox=(42.0, 26.0, 36.0, 45.0),
        start_date=start,
        end_date=today,
    )
    job_id, dedup = submit_firms_ingest(req)
    return job_id, dedup, start, today


def get_firms_metrics(hours: int = 24) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    with SessionLocal() as db:
        fires_count = db.execute(
            select(FireEventORM).where(FireEventORM.created_at >= since)
        ).scalars().all()
        alerts_count = db.execute(
            select(NotificationORM).where(NotificationORM.created_at >= since)
        ).scalars().all()
        jobs = db.execute(select(FirmsIngestJobORM).where(FirmsIngestJobORM.created_at >= since)).scalars().all()
    return {
        "fires_ingested_last_24h": len(fires_count),
        "alerts_generated_last_24h": len(alerts_count),
        "firms_jobs_last_24h": len(jobs),
    }


def list_notifications(portfolio_id: str | None = None) -> list[NotificationORM]:
    with SessionLocal() as db:
        stmt = select(NotificationORM)
        if portfolio_id:
            stmt = stmt.where(NotificationORM.portfolio_id == portfolio_id)
        rows = db.execute(stmt.order_by(desc(NotificationORM.created_at))).scalars().all()
    return rows


def ack_notification(notification_id: str) -> dict[str, Any] | None:
    with SessionLocal() as db:
        row = db.get(NotificationORM, notification_id)
        if not row:
            return None
        if row.acknowledged_at is None:
            row.acknowledged_at = datetime.now(timezone.utc)
            db.commit()
        return {"id": row.id, "acknowledged_at": row.acknowledged_at}


def get_asset_wildfire_features(asset_id: str, window: str) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        asset = db.execute(
            select(PortfolioAssetORM).where(PortfolioAssetORM.asset_id == asset_id).limit(1)
        ).scalar_one_or_none()
    if not asset:
        return None
    fires = load_fire_events_frame(now - timedelta(days=7), now)
    feat = wildfire_features_for_point(lat=float(asset.lat), lon=float(asset.lon), events_df=fires, window_end=now)
    if window == "24h":
        return {
            "nearest_fire_distance_km": feat["nearest_fire_distance_km_24h"],
            "fires_within_10km_count": feat["fires_within_10km_count_24h"],
            "max_frp_within_20km": feat["max_frp_within_20km_24h"],
        }
    return {
        "nearest_fire_distance_km": feat["nearest_fire_distance_km_7d"],
        "fires_within_10km_count": feat["fires_within_10km_count_7d"],
        "max_frp_within_20km": feat["max_frp_within_20km_7d"],
    }
