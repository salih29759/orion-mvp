from __future__ import annotations

from calendar import monthrange
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import logging
import math
from pathlib import Path
import tempfile
import threading
import time
from typing import Any
from uuid import uuid4

from google.cloud import storage
import pandas as pd
import requests
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import OpenaqIngestJobORM, ProvinceORM

LOG = logging.getLogger("orion.openaq")

OPENAQ_TURKEY_COUNTRY_ID = 223
DISCOVERY_PARAMETER_IDS = [1, 2, 3, 6]  # pm10, pm25, o3, no2
TARGET_PARAMETERS = ("pm25", "no2", "o3")
OUTPUT_COLUMNS = [
    "date",
    "station_id",
    "province_id",
    "lat",
    "lon",
    "pm25_measured_ugm3",
    "no2_measured_ugm3",
    "o3_measured_ugm3",
    "measurement_count",
    "coverage_pct",
    "source",
    "ingested_at",
]
METADATA_OBJECT_NAME = "metadata/openaq_turkey_stations.json"


class TokenBucket:
    def __init__(self, tokens_per_minute: int):
        rpm = max(int(tokens_per_minute), 1)
        self.capacity = float(rpm)
        self.refill_rate = float(rpm) / 60.0
        self.tokens = float(rpm)
        self.updated_at = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = max(now - self.updated_at, 0.0)
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                missing = 1.0 - self.tokens
                wait_seconds = missing / max(self.refill_rate, 1e-6)
            time.sleep(min(max(wait_seconds, 0.05), 1.0))


_RATE_LIMITER: TokenBucket | None = None
_RATE_LIMITER_RPM: int | None = None
_RATE_LIMITER_LOCK = threading.Lock()


def _get_rate_limiter() -> TokenBucket:
    global _RATE_LIMITER, _RATE_LIMITER_RPM
    rpm = max(int(settings.openaq_requests_per_minute), 1)
    with _RATE_LIMITER_LOCK:
        if _RATE_LIMITER is None or _RATE_LIMITER_RPM != rpm:
            _RATE_LIMITER = TokenBucket(tokens_per_minute=rpm)
            _RATE_LIMITER_RPM = rpm
        return _RATE_LIMITER


@dataclass
class StationInfo:
    station_id: str
    name: str | None
    lat: float
    lon: float
    province_id: str
    sensors: dict[str, list[int]]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_today() -> date:
    return _utc_now().date()


def _yesterday_utc() -> date:
    return _utc_today() - timedelta(days=1)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and not math.isnan(value):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _parse_datetime_utc(raw: Any) -> datetime | None:
    if raw in (None, ""):
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _measurement_hour_utc(measurement: dict[str, Any]) -> datetime | None:
    period = measurement.get("period") or {}
    dt_from = (period.get("datetimeFrom") or {}).get("utc")
    dt_to = (period.get("datetimeTo") or {}).get("utc")
    ts = _parse_datetime_utc(dt_from) or _parse_datetime_utc(dt_to)
    if ts is None:
        return None
    return ts.replace(minute=0, second=0, microsecond=0)


def _normalize_unit(unit: Any) -> str:
    text = str(unit or "").strip().lower()
    if not text:
        return ""
    text = text.replace("µ", "u").replace("μ", "u")
    text = text.replace("³", "3")
    text = text.replace("^", "")
    text = text.replace(" ", "")
    return text


def _is_ugm3(unit: Any) -> bool:
    normalized = _normalize_unit(unit)
    return normalized == "ug/m3"


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(1e-12, 1 - a)))
    return r * c


def _iter_month_ranges(start_date: date, end_date: date) -> list[tuple[str, date, date]]:
    ranges: list[tuple[str, date, date]] = []
    cur = date(start_date.year, start_date.month, 1)
    while cur <= end_date:
        y, m = cur.year, cur.month
        month_start = date(y, m, 1)
        month_end = date(y, m, monthrange(y, m)[1])
        ranges.append((f"{y:04d}-{m:02d}", max(start_date, month_start), min(end_date, month_end)))
        if m == 12:
            cur = date(y + 1, 1, 1)
        else:
            cur = date(y, m + 1, 1)
    return ranges


def _storage_client() -> storage.Client:
    return storage.Client()


def _progress_template() -> dict[str, Any]:
    return {
        "months": [],
        "recent_errors": [],
        "warnings": {
            "skipped_non_ugm3": 0,
            "skipped_flagged_total": 0,
        },
    }


def _parse_progress(progress_json: str | None) -> dict[str, Any]:
    if not progress_json:
        return _progress_template()
    try:
        parsed = json.loads(progress_json)
    except json.JSONDecodeError:
        return _progress_template()
    if not isinstance(parsed, dict):
        return _progress_template()
    if not isinstance(parsed.get("months"), list):
        parsed["months"] = []
    if not isinstance(parsed.get("recent_errors"), list):
        parsed["recent_errors"] = []
    warnings = parsed.get("warnings")
    if not isinstance(warnings, dict):
        warnings = {}
    warnings.setdefault("skipped_non_ugm3", 0)
    warnings.setdefault("skipped_flagged_total", 0)
    parsed["warnings"] = warnings
    return parsed


def _openaq_signature(*, start_date: date, requested_end_date: date, effective_end_date: date, concurrency: int) -> str:
    payload = {
        "start_date": start_date.isoformat(),
        "requested_end_date": requested_end_date.isoformat(),
        "effective_end_date": effective_end_date.isoformat(),
        "concurrency": int(concurrency),
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _load_province_centroids() -> list[tuple[str, float, float]]:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM.id, ProvinceORM.lat, ProvinceORM.lng)).all()
    return [(str(row[0]), float(row[1]), float(row[2])) for row in rows]


def _nearest_province_id(lat: float, lon: float, provinces: list[tuple[str, float, float]]) -> str:
    if not provinces:
        raise RuntimeError("Province centroid table is empty")
    best_id = provinces[0][0]
    best_dist = float("inf")
    for province_id, p_lat, p_lon in provinces:
        d = _haversine_km(lat, lon, p_lat, p_lon)
        if d < best_dist:
            best_dist = d
            best_id = province_id
    return best_id


def _upload_json_to_gcs(payload: dict[str, Any], object_name: str) -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    client = _storage_client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_string(json.dumps(payload, ensure_ascii=True, default=str), content_type="application/json")
    return f"gs://{settings.era5_gcs_bucket}/{object_name}"


def _upload_parquet_to_gcs(df: pd.DataFrame, object_name: str) -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    local = Path(tempfile.gettempdir()) / f"orion_openaq_{uuid4().hex}.parquet"
    df.to_parquet(local, index=False)
    client = _storage_client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_filename(str(local))
    return f"gs://{settings.era5_gcs_bucket}/{object_name}"


class OpenaqApiClient:
    def __init__(self):
        self.base_url = settings.openaq_base_url.rstrip("/")
        self.timeout_seconds = max(int(settings.openaq_timeout_seconds), 1)
        self.headers = {
            "Accept": "application/json",
            "X-API-Key": settings.openaq_api_key or "",
        }
        self.rate_limiter = _get_rate_limiter()
        self._request_count = 0
        self._request_count_lock = threading.Lock()

    def request_count(self) -> int:
        with self._request_count_lock:
            return self._request_count

    def _track_request(self) -> None:
        with self._request_count_lock:
            self._request_count += 1

    def _retry_wait(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            parsed = _coerce_int(retry_after)
            if parsed is not None:
                return float(max(parsed, 1))
        return float(min(60, 2 ** attempt))

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        attempts = 0
        max_attempts = 6
        while True:
            self.rate_limiter.acquire()
            self._track_request()
            try:
                response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout_seconds)
            except requests.RequestException as exc:
                if attempts >= max_attempts - 1:
                    raise RuntimeError(f"OpenAQ request failed for {path}: {exc}") from exc
                time.sleep(self._retry_wait(attempts + 1, None))
                attempts += 1
                continue

            if response.status_code < 400:
                try:
                    return response.json()
                except ValueError as exc:
                    raise RuntimeError(f"OpenAQ returned non-JSON for {path}") from exc

            if response.status_code == 401:
                raise RuntimeError("OpenAQ authentication failed (X-API-Key is invalid or missing)")

            if response.status_code in {429, 500, 502, 503, 504} and attempts < max_attempts - 1:
                wait_seconds = self._retry_wait(attempts + 1, response.headers.get("Retry-After"))
                time.sleep(wait_seconds)
                attempts += 1
                continue

            snippet = response.text[:500] if response.text else ""
            raise RuntimeError(f"OpenAQ request failed ({response.status_code}) path={path} body={snippet}")

    def _paginate(self, path: str, *, params: dict[str, Any], limit: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._get_json(path, {**params, "limit": limit, "page": page})
            rows = payload.get("results") or []
            if not isinstance(rows, list):
                raise RuntimeError(f"OpenAQ payload malformed for {path}: results is not a list")
            out.extend(row for row in rows if isinstance(row, dict))
            if not rows:
                break
            found = _coerce_int((payload.get("meta") or {}).get("found"))
            if found is not None and page * limit >= found:
                break
            page += 1
        return out

    def discover_turkey_locations(self) -> list[dict[str, Any]]:
        return self._paginate(
            "/locations",
            params={
                "countries_id": OPENAQ_TURKEY_COUNTRY_ID,
                "parameters_id": DISCOVERY_PARAMETER_IDS,
                "order_by": "id",
                "sort_order": "asc",
            },
            limit=1000,
        )

    def get_sensor_measurements(self, *, sensor_id: int, date_from_iso: str, date_to_iso: str) -> list[dict[str, Any]]:
        return self._paginate(
            f"/sensors/{sensor_id}/measurements",
            params={
                "datetime_from": date_from_iso,
                "datetime_to": date_to_iso,
            },
            limit=10000,
        )


def _build_stations(locations: list[dict[str, Any]], provinces: list[tuple[str, float, float]]) -> list[StationInfo]:
    stations: list[StationInfo] = []
    for location in locations:
        location_id = _coerce_int(location.get("id"))
        if location_id is None:
            continue
        coordinates = location.get("coordinates") or {}
        try:
            lat = float(coordinates.get("latitude"))
            lon = float(coordinates.get("longitude"))
        except (TypeError, ValueError):
            continue

        sensors: dict[str, list[int]] = {"pm10": [], "pm25": [], "o3": [], "no2": []}
        for sensor in location.get("sensors") or []:
            if not isinstance(sensor, dict):
                continue
            sensor_id = _coerce_int(sensor.get("id"))
            parameter = sensor.get("parameter") or {}
            parameter_name = str(parameter.get("name") or "").strip().lower()
            if sensor_id is None or parameter_name not in sensors:
                continue
            sensors[parameter_name].append(sensor_id)

        if not any(sensors.get(name) for name in ("pm10", "pm25", "o3", "no2")):
            continue

        province_id = _nearest_province_id(lat, lon, provinces)
        stations.append(
            StationInfo(
                station_id=str(location_id),
                name=str(location.get("name")) if location.get("name") is not None else None,
                lat=lat,
                lon=lon,
                province_id=province_id,
                sensors=sensors,
            )
        )

    # Stable ordering makes metadata diffs deterministic.
    stations.sort(key=lambda s: int(s.station_id))
    return stations


def _station_metadata_payload(stations: list[StationInfo]) -> dict[str, Any]:
    counts_by_parameter = {
        "pm10": sum(1 for s in stations if s.sensors.get("pm10")),
        "pm25": sum(1 for s in stations if s.sensors.get("pm25")),
        "o3": sum(1 for s in stations if s.sensors.get("o3")),
        "no2": sum(1 for s in stations if s.sensors.get("no2")),
    }
    return {
        "generated_at": _utc_now().isoformat(),
        "source": "openaq_v3",
        "countries_id": OPENAQ_TURKEY_COUNTRY_ID,
        "parameters_id": DISCOVERY_PARAMETER_IDS,
        "stations_total": len(stations),
        "counts_by_parameter": counts_by_parameter,
        "stations": [
            {
                "station_id": station.station_id,
                "name": station.name,
                "lat": station.lat,
                "lon": station.lon,
                "province_id": station.province_id,
                "sensors": station.sensors,
            }
            for station in stations
        ],
    }


def _measurement_to_row(
    measurement: dict[str, Any],
    *,
    default_parameter: str,
) -> tuple[dict[str, Any] | None, str | None]:
    parameter = measurement.get("parameter") or {}
    parameter_name = str(parameter.get("name") or default_parameter).strip().lower()
    if parameter_name not in TARGET_PARAMETERS:
        return None, "unsupported_parameter"

    if bool((measurement.get("flagInfo") or {}).get("hasFlags")):
        return None, "flagged"

    if not _is_ugm3(parameter.get("units")):
        return None, "non_ugm3"

    ts_hour = _measurement_hour_utc(measurement)
    if ts_hour is None:
        return None, "invalid_timestamp"

    try:
        value = float(measurement.get("value"))
    except (TypeError, ValueError):
        return None, "invalid_value"

    return {
        "parameter": parameter_name,
        "hour": ts_hour,
        "value": value,
    }, None


def _aggregate_station_day_rows(
    station: StationInfo,
    measurement_rows: list[dict[str, Any]],
    *,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    if not measurement_rows:
        return []

    frame = pd.DataFrame(measurement_rows)
    if frame.empty:
        return []

    frame["date"] = pd.to_datetime(frame["hour"], utc=True).dt.date
    hourly = frame.groupby(["date", "hour", "parameter"], as_index=False)["value"].mean()
    daily_param = hourly.groupby(["date", "parameter"], as_index=False)["value"].mean()
    pivot = daily_param.pivot(index="date", columns="parameter", values="value").reset_index()
    coverage = hourly.groupby(["date"], as_index=False)["hour"].nunique().rename(columns={"hour": "measurement_count"})
    merged = coverage.merge(pivot, on="date", how="left")

    for parameter_name in TARGET_PARAMETERS:
        if parameter_name not in merged.columns:
            merged[parameter_name] = pd.NA

    merged = merged.sort_values("date")
    out: list[dict[str, Any]] = []
    for row in merged.to_dict(orient="records"):
        measurement_count = int(row.get("measurement_count") or 0)
        out.append(
            {
                "date": row["date"],
                "station_id": station.station_id,
                "province_id": station.province_id,
                "lat": float(station.lat),
                "lon": float(station.lon),
                "pm25_measured_ugm3": float(row["pm25"]) if pd.notna(row.get("pm25")) else None,
                "no2_measured_ugm3": float(row["no2"]) if pd.notna(row.get("no2")) else None,
                "o3_measured_ugm3": float(row["o3"]) if pd.notna(row.get("o3")) else None,
                "measurement_count": measurement_count,
                "coverage_pct": (measurement_count / 24.0) * 100.0,
                "source": "openaq_v3",
                "ingested_at": ingested_at,
            }
        )
    return out


def _process_station_for_window(
    client: OpenaqApiClient,
    station: StationInfo,
    *,
    start_date: date,
    end_date: date,
    ingested_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    date_from_iso = f"{start_date.isoformat()}T00:00:00Z"
    date_to_iso = f"{end_date.isoformat()}T23:59:59Z"
    warnings = {
        "skipped_non_ugm3": 0,
        "skipped_flagged_total": 0,
    }

    measurement_rows: list[dict[str, Any]] = []
    for parameter_name in TARGET_PARAMETERS:
        for sensor_id in station.sensors.get(parameter_name, []):
            rows = client.get_sensor_measurements(
                sensor_id=int(sensor_id),
                date_from_iso=date_from_iso,
                date_to_iso=date_to_iso,
            )
            for measurement in rows:
                parsed, skip_reason = _measurement_to_row(measurement, default_parameter=parameter_name)
                if skip_reason == "non_ugm3":
                    warnings["skipped_non_ugm3"] += 1
                    continue
                if skip_reason == "flagged":
                    warnings["skipped_flagged_total"] += 1
                    continue
                if not parsed:
                    continue
                ts_hour = parsed["hour"]
                if ts_hour.date() < start_date or ts_hour.date() > end_date:
                    continue
                measurement_rows.append(parsed)

    return _aggregate_station_day_rows(station, measurement_rows, ingested_at=ingested_at), warnings


def _empty_month_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _merge_warnings(target: dict[str, int], incoming: dict[str, int]) -> None:
    for key, value in incoming.items():
        target[key] = int(target.get(key, 0)) + int(value)


def _append_recent_error(progress: dict[str, Any], *, month: str, station_id: str | None, error: str) -> None:
    recent = progress.get("recent_errors")
    if not isinstance(recent, list):
        recent = []
        progress["recent_errors"] = recent
    recent.append(
        {
            "month": month,
            "station_id": station_id,
            "error": error,
            "at": _utc_now().isoformat(),
        }
    )
    # Keep payload bounded.
    if len(recent) > 50:
        del recent[:-50]


def _month_payload(
    *,
    month_label: str,
    status: str,
    row_count: int,
    gcs_uri: str | None,
    station_failures: int,
) -> dict[str, Any]:
    payload = {
        "month": month_label,
        "status": status,
        "rows_written": row_count,
        "station_failures": station_failures,
    }
    if gcs_uri:
        payload["gcs_uri"] = gcs_uri
    return payload


def validate_openaq_runtime() -> list[str]:
    missing: list[str] = []
    if not settings.openaq_api_key:
        missing.append("OPENAQ_API_KEY")
    if not settings.era5_gcs_bucket:
        missing.append("ERA5_GCS_BUCKET")
    return missing


def submit_openaq_backfill(start_date: date, end_date: date, concurrency: int) -> tuple[str, bool, int]:
    requested_end_date = end_date
    effective_end_date = min(requested_end_date, _yesterday_utc())
    if start_date > effective_end_date:
        raise ValueError(
            f"start ({start_date.isoformat()}) must be <= effective_end ({effective_end_date.isoformat()})"
        )

    normalized_concurrency = max(1, min(int(concurrency), 10))
    request_sig = _openaq_signature(
        start_date=start_date,
        requested_end_date=requested_end_date,
        effective_end_date=effective_end_date,
        concurrency=normalized_concurrency,
    )
    months_total = len(_iter_month_ranges(start_date, effective_end_date))

    with SessionLocal() as db:
        existing = db.execute(
            select(OpenaqIngestJobORM).where(OpenaqIngestJobORM.request_signature == request_sig).limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing.job_id, True, int(existing.months_total)

        job_id = str(uuid4())
        db.add(
            OpenaqIngestJobORM(
                job_id=job_id,
                request_signature=request_sig,
                status="queued",
                start_date=start_date,
                requested_end_date=requested_end_date,
                effective_end_date=effective_end_date,
                concurrency=normalized_concurrency,
                months_total=months_total,
                months_completed=0,
                months_failed=0,
                stations_total=0,
                stations_processed=0,
                rows_written=0,
                api_requests=0,
                progress_json=json.dumps(_progress_template()),
            )
        )
        db.commit()

    return job_id, False, months_total


def get_openaq_job(job_id: str) -> OpenaqIngestJobORM | None:
    with SessionLocal() as db:
        return db.get(OpenaqIngestJobORM, job_id)


def get_latest_openaq_job() -> OpenaqIngestJobORM | None:
    with SessionLocal() as db:
        return db.execute(select(OpenaqIngestJobORM).order_by(desc(OpenaqIngestJobORM.created_at)).limit(1)).scalar_one_or_none()


def openaq_job_to_status_payload(job: OpenaqIngestJobORM) -> dict[str, Any]:
    progress = _parse_progress(job.progress_json)
    job_status = str(job.status)
    running_count = 1 if job_status == "running" else 0
    pending = max(int(job.months_total) - int(job.months_completed) - int(job.months_failed) - running_count, 0)
    last_updated = job.finished_at or job.started_at or job.created_at
    warnings = progress.get("warnings") if isinstance(progress.get("warnings"), dict) else {}

    return {
        "run_id": job.job_id,
        "status": job_status,
        "total_months": int(job.months_total),
        "completed": int(job.months_completed),
        "failed": int(job.months_failed),
        "running": running_count,
        "pending": pending,
        "rows_written": int(job.rows_written),
        "requested_start": job.start_date,
        "requested_end": job.requested_end_date,
        "effective_end": job.effective_end_date,
        "stations_total": int(job.stations_total),
        "stations_processed": int(job.stations_processed),
        "metadata_gcs_uri": job.metadata_gcs_uri,
        "last_updated": last_updated,
        "recent_errors": progress.get("recent_errors") if isinstance(progress.get("recent_errors"), list) else [],
        "warnings": {
            "skipped_non_ugm3": int(warnings.get("skipped_non_ugm3", 0)),
            "skipped_flagged_total": int(warnings.get("skipped_flagged_total", 0)),
        },
    }


def _save_job_progress(
    *,
    job_id: str,
    status: str | None = None,
    months_completed: int | None = None,
    months_failed: int | None = None,
    rows_written: int | None = None,
    stations_total: int | None = None,
    stations_processed: int | None = None,
    metadata_gcs_uri: str | None = None,
    api_requests: int | None = None,
    progress: dict[str, Any] | None = None,
    error: str | None = None,
    finished_at: datetime | None = None,
) -> None:
    with SessionLocal() as db:
        job = db.get(OpenaqIngestJobORM, job_id)
        if not job:
            return
        if status is not None:
            job.status = status
        if months_completed is not None:
            job.months_completed = months_completed
        if months_failed is not None:
            job.months_failed = months_failed
        if rows_written is not None:
            job.rows_written = rows_written
        if stations_total is not None:
            job.stations_total = stations_total
        if stations_processed is not None:
            job.stations_processed = stations_processed
        if metadata_gcs_uri is not None:
            job.metadata_gcs_uri = metadata_gcs_uri
        if api_requests is not None:
            job.api_requests = api_requests
        if progress is not None:
            job.progress_json = json.dumps(progress, ensure_ascii=True, default=str)
        if error is not None:
            job.error = error
        if finished_at is not None:
            job.finished_at = finished_at
        db.commit()


def process_openaq_job(job_id: str) -> None:
    with SessionLocal() as db:
        job = db.get(OpenaqIngestJobORM, job_id)
        if not job:
            return
        job.status = "running"
        job.started_at = _utc_now()
        db.commit()
        start_date = job.start_date
        effective_end_date = job.effective_end_date
        concurrency = int(job.concurrency)
        rows_written = int(job.rows_written or 0)
        months_completed = int(job.months_completed or 0)
        months_failed = int(job.months_failed or 0)
        progress = _parse_progress(job.progress_json)

    client = OpenaqApiClient()
    latest_error: str | None = None
    processed_station_ids: set[str] = set()

    try:
        provinces = _load_province_centroids()
        locations = client.discover_turkey_locations()
        stations = _build_stations(locations, provinces)

        metadata_payload = _station_metadata_payload(stations)
        metadata_gcs_uri = _upload_json_to_gcs(metadata_payload, METADATA_OBJECT_NAME)

        if len(stations) < 100:
            raise RuntimeError(f"OpenAQ station discovery returned {len(stations)} stations (<100)")

        _save_job_progress(
            job_id=job_id,
            stations_total=len(stations),
            metadata_gcs_uri=metadata_gcs_uri,
            api_requests=client.request_count(),
            progress=progress,
        )

        for month_label, month_start, month_end in _iter_month_ranges(start_date, effective_end_date):
            ingested_at = _utc_now()
            month_rows: list[dict[str, Any]] = []
            month_warnings = {"skipped_non_ugm3": 0, "skipped_flagged_total": 0}
            station_failures = 0

            futures: dict[Future[tuple[list[dict[str, Any]], dict[str, int]]], str] = {}
            worker_count = max(1, int(concurrency))
            with ThreadPoolExecutor(max_workers=worker_count) as pool:
                for station in stations:
                    fut = pool.submit(
                        _process_station_for_window,
                        client,
                        station,
                        start_date=month_start,
                        end_date=month_end,
                        ingested_at=ingested_at,
                    )
                    futures[fut] = station.station_id

                for fut in as_completed(futures):
                    station_id = futures[fut]
                    processed_station_ids.add(station_id)
                    try:
                        station_rows, warnings = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        station_failures += 1
                        latest_error = str(exc)
                        _append_recent_error(progress, month=month_label, station_id=station_id, error=str(exc))
                        LOG.warning("openaq station processing failed station_id=%s month=%s error=%s", station_id, month_label, str(exc))
                        continue

                    month_rows.extend(station_rows)
                    _merge_warnings(month_warnings, warnings)

            _merge_warnings(progress["warnings"], month_warnings)

            month_frame = _empty_month_frame() if not month_rows else pd.DataFrame(month_rows, columns=OUTPUT_COLUMNS)
            month_uri = _upload_parquet_to_gcs(
                month_frame,
                f"features/daily/openaq/year={month_start.year:04d}/month={month_start.month:02d}/part-0.parquet",
            )
            month_row_count = int(len(month_frame))
            rows_written += month_row_count

            month_status = "completed"
            if station_failures > 0:
                month_status = "completed_with_failures"
                months_failed += 1
            else:
                months_completed += 1

            progress["months"].append(
                _month_payload(
                    month_label=month_label,
                    status=month_status,
                    row_count=month_row_count,
                    gcs_uri=month_uri,
                    station_failures=station_failures,
                )
            )

            _save_job_progress(
                job_id=job_id,
                months_completed=months_completed,
                months_failed=months_failed,
                rows_written=rows_written,
                stations_processed=len(processed_station_ids),
                api_requests=client.request_count(),
                progress=progress,
            )

    except Exception as exc:  # noqa: BLE001
        latest_error = str(exc)
        LOG.exception("openaq job failed job_id=%s error=%s", job_id, latest_error)

    final_status = "completed"
    if months_failed > 0 and months_completed == 0:
        final_status = "failed"
    elif months_failed > 0 and months_completed > 0:
        final_status = "completed_with_failures"

    if latest_error and final_status == "completed":
        final_status = "failed"

    _save_job_progress(
        job_id=job_id,
        status=final_status,
        rows_written=rows_written,
        stations_processed=len(processed_station_ids),
        api_requests=client.request_count(),
        progress=progress,
        error=latest_error,
        finished_at=_utc_now(),
    )


def start_openaq_background_job(job_id: str) -> None:
    worker = threading.Thread(target=process_openaq_job, args=(job_id,), daemon=False)
    worker.start()


def run_daily_openaq_update() -> dict[str, Any]:
    day = _yesterday_utc()
    job_id, deduped, months_total = submit_openaq_backfill(
        start_date=day,
        end_date=day,
        concurrency=max(1, int(settings.openaq_daily_concurrency)),
    )
    if not deduped:
        start_openaq_background_job(job_id)
    return {
        "status": "accepted",
        "job_id": job_id,
        "deduplicated": deduped,
        "start": day.isoformat(),
        "end": day.isoformat(),
        "months_total": months_total,
    }
