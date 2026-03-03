from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import tempfile
import threading
import time as wall_time
from typing import Any
from uuid import uuid4

from google.cloud import storage
import pandas as pd
import requests
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import EarthquakeIngestJobORM

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
AFAD_URL = "https://deprem.afad.gov.tr/apiv2/event/filter"
TURKEY_BBOX = {
    "minlatitude": 36.0,
    "maxlatitude": 42.0,
    "minlongitude": 26.0,
    "maxlongitude": 45.0,
}

ACTIVE_OR_SUCCESS_STATUSES = {"queued", "running", "success", "success_with_warnings"}
ACTIVE_STATUSES = {"queued", "running"}


@dataclass
class NormalizedEvent:
    raw_source: str
    native_id: str | None
    event_time_utc: datetime
    lat: float
    lon: float
    depth_km: float | None
    magnitude: float | None
    magnitude_type: str | None
    place: str | None


class _DisjointSet:
    def __init__(self, n: int):
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        parent = self.parent
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _storage_client() -> storage.Client:
    return storage.Client()


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:  # noqa: BLE001
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_afad_datetime(value: str) -> datetime:
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(1e-12, 1 - a)))
    return r * c


def _canonical_tuple_hash(*, dt: datetime, lat: float, lon: float, magnitude: float | None) -> str:
    dt_sec = dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    mag_value = "na" if magnitude is None else f"{round(float(magnitude), 1):.1f}"
    raw = f"{dt_sec}|{round(float(lat), 3):.3f}|{round(float(lon), 3):.3f}|{mag_value}"
    return sha256(raw.encode("utf-8")).hexdigest()


def build_day_partition_object_name(day_value: date) -> str:
    return (
        "reference/earthquakes/events/"
        f"year={day_value.year:04d}/month={day_value.month:02d}/day={day_value.day:02d}/part-0.parquet"
    )


def _request_signature(*, start: date, end: date, min_magnitude: float) -> str:
    payload = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "min_magnitude": round(float(min_magnitude), 2),
        "bbox": TURKEY_BBOX,
        "version": "eq-events-v1",
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _job_progress_template(*, start: date, end: date) -> dict[str, Any]:
    days_total = (end - start).days + 1
    return {
        "days_total": max(days_total, 0),
        "days_done": 0,
        "days_failed": 0,
        "failed_days": [],
        "rows_written_total": 0,
        "files_written_total": 0,
        "last_day_written": None,
        "last_updated": _utc_now().isoformat(),
        "day_summaries": [],
    }


def _persist_job_progress(
    *,
    job: EarthquakeIngestJobORM,
    progress: dict[str, Any],
    db,
) -> None:
    progress["last_updated"] = _utc_now().isoformat()
    job.progress_json = json.dumps(progress, ensure_ascii=True, separators=(",", ":"))
    job.rows_written = int(progress.get("rows_written_total", 0) or 0)
    job.files_written = int(progress.get("files_written_total", 0) or 0)


def _iter_days(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _normalize_usgs_feature(feature: dict[str, Any]) -> NormalizedEvent | None:
    props = feature.get("properties") or {}
    coords = (feature.get("geometry") or {}).get("coordinates") or []
    if len(coords) < 2:
        return None

    ts_ms = props.get("time")
    if ts_ms is None:
        return None

    try:
        event_time = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None

    lon = _coerce_float(coords[0])
    lat = _coerce_float(coords[1])
    depth = _coerce_float(coords[2]) if len(coords) >= 3 else None
    if lat is None or lon is None:
        return None

    return NormalizedEvent(
        raw_source="usgs",
        native_id=str(feature.get("id")) if feature.get("id") is not None else None,
        event_time_utc=event_time,
        lat=float(lat),
        lon=float(lon),
        depth_km=depth,
        magnitude=_coerce_float(props.get("mag")),
        magnitude_type=str(props.get("magType")) if props.get("magType") not in (None, "") else None,
        place=str(props.get("place")) if props.get("place") not in (None, "") else None,
    )


def _normalize_afad_item(item: dict[str, Any]) -> NormalizedEvent | None:
    dt_raw = item.get("date")
    if dt_raw in (None, ""):
        return None
    try:
        event_time = parse_afad_datetime(str(dt_raw))
    except Exception:  # noqa: BLE001
        return None

    lat = _coerce_float(item.get("latitude"))
    lon = _coerce_float(item.get("longitude"))
    if lat is None or lon is None:
        return None

    return NormalizedEvent(
        raw_source="afad",
        native_id=str(item.get("eventID")) if item.get("eventID") not in (None, "") else None,
        event_time_utc=event_time,
        lat=float(lat),
        lon=float(lon),
        depth_km=_coerce_float(item.get("depth")),
        magnitude=_coerce_float(item.get("magnitude")),
        magnitude_type=str(item.get("type")) if item.get("type") not in (None, "") else None,
        place=str(item.get("location")) if item.get("location") not in (None, "") else None,
    )


def fetch_usgs_events_for_day(*, day_value: date, min_magnitude: float, timeout_sec: int = 30) -> list[NormalizedEvent]:
    out: list[NormalizedEvent] = []
    start_dt = datetime.combine(day_value, time.min, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    limit = 500
    offset = 1
    with requests.Session() as session:
        while True:
            params = {
                "format": "geojson",
                "starttime": start_dt.isoformat().replace("+00:00", "Z"),
                "endtime": end_dt.isoformat().replace("+00:00", "Z"),
                "minmagnitude": f"{float(min_magnitude):.2f}",
                "orderby": "time",
                "limit": limit,
                "offset": offset,
                **TURKEY_BBOX,
            }
            resp = session.get(USGS_URL, params=params, timeout=timeout_sec)
            resp.raise_for_status()
            payload = resp.json()
            features = payload.get("features") or []
            for feature in features:
                normalized = _normalize_usgs_feature(feature)
                if normalized is not None:
                    out.append(normalized)

            if len(features) < limit:
                break

            offset += limit
            wall_time.sleep(1.0)

    return out


def fetch_afad_events_for_day(*, day_value: date, min_magnitude: float, timeout_sec: int = 30) -> list[NormalizedEvent]:
    out: list[NormalizedEvent] = []
    start_dt = datetime.combine(day_value, time.min, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    limit = 500
    offset = 0
    with requests.Session() as session:
        while True:
            params = {
                "start": start_dt.isoformat().replace("+00:00", "Z"),
                "end": end_dt.isoformat().replace("+00:00", "Z"),
                "minmag": f"{float(min_magnitude):.2f}",
                "limit": limit,
                "offset": offset,
                **TURKEY_BBOX,
            }
            resp = session.get(AFAD_URL, params=params, timeout=timeout_sec)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list):
                break

            for item in payload:
                normalized = _normalize_afad_item(item)
                if normalized is not None:
                    out.append(normalized)

            if len(payload) < limit:
                break

            offset += limit

    return out


def _is_duplicate(a: NormalizedEvent, b: NormalizedEvent) -> bool:
    dt_diff = abs((a.event_time_utc - b.event_time_utc).total_seconds())
    if dt_diff > 2.0:
        return False

    distance = _haversine_km(a.lat, a.lon, b.lat, b.lon)
    if distance > 25.0:
        return False

    if a.magnitude is None or b.magnitude is None:
        return False

    return abs(float(a.magnitude) - float(b.magnitude)) <= 0.3


def deduplicate_events(events: list[NormalizedEvent]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(
            columns=[
                "event_id",
                "event_time_utc",
                "lat",
                "lon",
                "depth_km",
                "magnitude",
                "magnitude_type",
                "place",
                "source_primary",
                "source_ids",
                "raw_source",
                "ingested_at_utc",
                "run_id",
                "dedup_group_id",
                "quality_flags",
            ]
        )

    dsu = _DisjointSet(len(events))
    for i in range(len(events)):
        for j in range(i + 1, len(events)):
            if _is_duplicate(events[i], events[j]):
                dsu.union(i, j)

    grouped: dict[int, list[NormalizedEvent]] = {}
    for i, ev in enumerate(events):
        root = dsu.find(i)
        grouped.setdefault(root, []).append(ev)

    rows: list[dict[str, Any]] = []
    now = _utc_now()
    for group_events in grouped.values():
        usgs_candidates = [e for e in group_events if e.raw_source == "usgs"]
        primary = usgs_candidates[0] if usgs_candidates else group_events[0]

        canonical_time = usgs_candidates[0].event_time_utc if usgs_candidates else min(e.event_time_utc for e in group_events)

        magnitude = usgs_candidates[0].magnitude if usgs_candidates and usgs_candidates[0].magnitude is not None else primary.magnitude
        if magnitude is None:
            fallback_mag = [e.magnitude for e in group_events if e.magnitude is not None]
            magnitude = fallback_mag[0] if fallback_mag else None

        source_ids: dict[str, str] = {}
        for ev in group_events:
            if ev.raw_source == "usgs" and ev.native_id:
                source_ids["usgs_id"] = ev.native_id
            if ev.raw_source == "afad" and ev.native_id:
                source_ids["afad_event_id"] = ev.native_id

        dedup_seed = "|".join(f"{k}:{v}" for k, v in sorted(source_ids.items()))
        if not dedup_seed:
            dedup_seed = _canonical_tuple_hash(dt=canonical_time, lat=primary.lat, lon=primary.lon, magnitude=magnitude)
        dedup_group_id = sha256(dedup_seed.encode("utf-8")).hexdigest()

        event_id = _canonical_tuple_hash(dt=canonical_time, lat=primary.lat, lon=primary.lon, magnitude=magnitude)
        quality_flags = []
        if magnitude is None:
            quality_flags.append("missing_magnitude")

        rows.append(
            {
                "event_id": event_id,
                "event_time_utc": canonical_time,
                "lat": float(primary.lat),
                "lon": float(primary.lon),
                "depth_km": primary.depth_km,
                "magnitude": magnitude,
                "magnitude_type": primary.magnitude_type,
                "place": primary.place,
                "source_primary": primary.raw_source,
                "source_ids": json.dumps(source_ids, ensure_ascii=True, sort_keys=True),
                "raw_source": group_events[0].raw_source,
                "ingested_at_utc": now,
                "run_id": "",
                "dedup_group_id": dedup_group_id,
                "quality_flags": json.dumps(quality_flags, ensure_ascii=True) if quality_flags else None,
            }
        )

    frame = pd.DataFrame(rows)
    frame = frame.sort_values(["event_time_utc", "event_id"]).reset_index(drop=True)
    return frame


def _write_day_parquet(*, day_value: date, frame: pd.DataFrame) -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    object_name = build_day_partition_object_name(day_value)
    local_path = Path(tempfile.gettempdir()) / f"orion_eq_{day_value.isoformat()}_{uuid4().hex}.parquet"
    frame.to_parquet(local_path, index=False)

    client = _storage_client()
    bucket = client.bucket(settings.era5_gcs_bucket)
    bucket.blob(object_name).upload_from_filename(str(local_path))
    return f"gs://{settings.era5_gcs_bucket}/{object_name}"


def _process_day(*, day_value: date, min_magnitude: float, run_id: str) -> tuple[int, int, str]:
    usgs_events = fetch_usgs_events_for_day(day_value=day_value, min_magnitude=min_magnitude)
    afad_events = fetch_afad_events_for_day(day_value=day_value, min_magnitude=min_magnitude)
    merged = deduplicate_events(usgs_events + afad_events)

    merged["run_id"] = run_id
    for col in (
        "event_id",
        "source_ids",
        "dedup_group_id",
        "source_primary",
        "raw_source",
    ):
        merged[col] = merged[col].astype(str)

    _uri = _write_day_parquet(day_value=day_value, frame=merged)
    return int(len(merged.index)), 1, _uri


def _update_day_summary(*, progress: dict[str, Any], day_value: date, rows: int, status: str, error: str | None) -> None:
    day_key = day_value.isoformat()
    summaries = list(progress.get("day_summaries") or [])
    summaries.append({"day": day_key, "status": status, "rows": rows, "error": error})
    progress["day_summaries"] = summaries[-120:]


def _validate_runtime() -> None:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")


def get_earthquake_job(job_id: str) -> EarthquakeIngestJobORM | None:
    with SessionLocal() as db:
        return db.get(EarthquakeIngestJobORM, job_id)


def get_latest_earthquake_job() -> EarthquakeIngestJobORM | None:
    with SessionLocal() as db:
        return db.execute(select(EarthquakeIngestJobORM).order_by(desc(EarthquakeIngestJobORM.created_at)).limit(1)).scalar_one_or_none()


def earthquake_job_to_status_payload(job: EarthquakeIngestJobORM) -> dict[str, Any]:
    progress = {}
    if job.progress_json:
        try:
            progress = json.loads(job.progress_json)
        except json.JSONDecodeError:
            progress = {"raw": job.progress_json}

    return {
        "job_id": job.job_id,
        "status": job.status,
        "type": "earthquakes_backfill",
        "created_at": job.created_at,
        "updated_at": job.finished_at or job.started_at,
        "progress": progress,
        "children": [],
    }


def run_earthquake_backfill(*, job_id: str, start_date: date, end_date: date, min_magnitude: float) -> None:
    with SessionLocal() as db:
        job = db.get(EarthquakeIngestJobORM, job_id)
        if not job:
            return

        job.status = "running"
        job.started_at = _utc_now()
        db.commit()

    progress = _job_progress_template(start=start_date, end=end_date)

    for day_value in _iter_days(start_date, end_date):
        with SessionLocal() as db:
            job = db.get(EarthquakeIngestJobORM, job_id)
            if not job:
                return
            try:
                rows_written, files_written, _ = _process_day(day_value=day_value, min_magnitude=min_magnitude, run_id=job_id)
                progress["days_done"] = int(progress.get("days_done", 0)) + 1
                progress["rows_written_total"] = int(progress.get("rows_written_total", 0)) + rows_written
                progress["files_written_total"] = int(progress.get("files_written_total", 0)) + files_written
                progress["last_day_written"] = day_value.isoformat()
                _update_day_summary(progress=progress, day_value=day_value, rows=rows_written, status="success", error=None)
            except Exception as exc:  # noqa: BLE001
                progress["days_failed"] = int(progress.get("days_failed", 0)) + 1
                failed_days = list(progress.get("failed_days") or [])
                failed_days.append({"day": day_value.isoformat(), "error": str(exc)[:400]})
                progress["failed_days"] = failed_days[-40:]
                _update_day_summary(progress=progress, day_value=day_value, rows=0, status="failed", error=str(exc)[:400])

            _persist_job_progress(job=job, progress=progress, db=db)
            db.commit()

    with SessionLocal() as db:
        job = db.get(EarthquakeIngestJobORM, job_id)
        if not job:
            return

        days_total = int(progress.get("days_total", 0))
        days_done = int(progress.get("days_done", 0))
        days_failed = int(progress.get("days_failed", 0))
        if days_failed == 0 and days_done == days_total:
            final_status = "success"
        elif days_done > 0 and days_failed > 0:
            final_status = "success_with_warnings"
        else:
            final_status = "failed"

        job.status = final_status
        if final_status == "failed":
            latest_failures = list(progress.get("failed_days") or [])
            if latest_failures:
                job.error = str(latest_failures[-1].get("error"))[:400]
        job.finished_at = _utc_now()
        _persist_job_progress(job=job, progress=progress, db=db)
        db.commit()


def submit_earthquake_backfill(*, start_date: date, end_date: date, min_magnitude: float) -> tuple[str, bool, date]:
    _validate_runtime()

    yesterday_utc = (_utc_now() - timedelta(days=1)).date()
    effective_end = min(end_date, yesterday_utc)
    if start_date > effective_end:
        raise ValueError("start must be <= effective_end (yesterday UTC)")

    signature = _request_signature(start=start_date, end=effective_end, min_magnitude=min_magnitude)

    with SessionLocal() as db:
        existing = db.execute(
            select(EarthquakeIngestJobORM)
            .where(
                EarthquakeIngestJobORM.request_signature == signature,
                EarthquakeIngestJobORM.status.in_(ACTIVE_OR_SUCCESS_STATUSES),
            )
            .order_by(desc(EarthquakeIngestJobORM.created_at))
            .limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing.job_id, True, effective_end

        job_id = f"eq_{uuid4().hex[:12]}"
        progress = _job_progress_template(start=start_date, end=effective_end)
        row = EarthquakeIngestJobORM(
            job_id=job_id,
            request_signature=signature,
            status="queued",
            start_date=start_date,
            end_date=effective_end,
            rows_written=0,
            files_written=0,
            progress_json=json.dumps(progress, ensure_ascii=True, separators=(",", ":")),
        )
        db.add(row)
        db.commit()

    thread = threading.Thread(
        target=run_earthquake_backfill,
        kwargs={
            "job_id": job_id,
            "start_date": start_date,
            "end_date": effective_end,
            "min_magnitude": float(min_magnitude),
        },
        daemon=False,
    )
    thread.start()

    return job_id, False, effective_end


def run_earthquake_daily_update(*, min_magnitude: float = 2.5) -> dict[str, Any]:
    target_day = (_utc_now() - timedelta(days=1)).date()
    job_id, deduped, _effective_end = submit_earthquake_backfill(
        start_date=target_day,
        end_date=target_day,
        min_magnitude=min_magnitude,
    )
    return {
        "status": "accepted",
        "job_id": job_id,
        "target_date": target_day.isoformat(),
        "deduplicated": deduped,
    }
