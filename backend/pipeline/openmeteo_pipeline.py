from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import logging
from pathlib import Path
import tempfile
import threading
from typing import Any
from uuid import uuid4

from google.cloud import storage
import numpy as np
import pandas as pd
import requests
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import OpenMeteoArtifactORM, OpenMeteoJobORM, ProvinceORM

LOG = logging.getLogger("orion.openmeteo")

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE_NAME = "Europe/Istanbul"
MODEL_NAME = "best_match"

JOB_TYPE_BACKFILL = "openmeteo_backfill"
JOB_TYPE_FORECAST = "openmeteo_forecast"
JOB_TYPE_DAILY = "openmeteo_daily"

DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "et0_fao_evapotranspiration",
    "soil_moisture_0_to_10cm_mean",
    "shortwave_radiation_sum",
]


@dataclass
class ProvincePoint:
    point_id: str
    lat: float
    lon: float


@dataclass
class PartitionWriteResult:
    rows_written: int
    files_written: int
    artifacts: list[dict[str, Any]]


def kmh_to_ms(values: pd.Series) -> pd.Series:
    raw = pd.to_numeric(values, errors="coerce")
    return raw / 3.6


def mj_per_day_to_wm2(values: pd.Series) -> pd.Series:
    raw = pd.to_numeric(values, errors="coerce")
    return raw * 1_000_000.0 / 86_400.0


def merge_partition_frames(existing_df: pd.DataFrame, new_df: pd.DataFrame, dedupe_keys: list[str]) -> pd.DataFrame:
    if existing_df is None or existing_df.empty:
        merged = new_df.copy()
    else:
        merged = pd.concat([existing_df, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=dedupe_keys, keep="last")

    sort_cols = [c for c in ["date", "point_id", "target_date", "horizon_days"] if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols).reset_index(drop=True)
    return merged


def compute_spi_proxy_30d(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.sort_values(["point_id", "date"]).copy()
    out["precip_sum_mm"] = pd.to_numeric(out["precip_sum_mm"], errors="coerce")

    rolling_mean = out.groupby("point_id")["precip_sum_mm"].transform(lambda s: s.rolling(30, min_periods=30).mean())
    rolling_std = out.groupby("point_id")["precip_sum_mm"].transform(lambda s: s.rolling(30, min_periods=30).std(ddof=0))
    rolling_std = rolling_std.replace(0, np.nan)
    out["spi_proxy_30d"] = (out["precip_sum_mm"] - rolling_mean) / rolling_std
    return out


def _file_sha256(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _storage_client() -> storage.Client:
    return storage.Client()


def _ensure_bucket() -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    return settings.era5_gcs_bucket


def _download_blob_to_frame(blob) -> pd.DataFrame:
    tmp = Path(tempfile.gettempdir()) / f"openmeteo_existing_{uuid4().hex}.parquet"
    blob.download_to_filename(str(tmp))
    try:
        return pd.read_parquet(tmp)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _upload_partition(
    *,
    df: pd.DataFrame,
    object_name: str,
    dedupe_keys: list[str] | None,
) -> tuple[pd.DataFrame, str, str, int]:
    bucket_name = _ensure_bucket()
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    to_store = df.copy()
    if dedupe_keys and blob.exists(client):
        existing = _download_blob_to_frame(blob)
        to_store = merge_partition_frames(existing, to_store, dedupe_keys=dedupe_keys)

    tmp = Path(tempfile.gettempdir()) / f"openmeteo_upload_{uuid4().hex}.parquet"
    to_store.to_parquet(tmp, index=False)
    checksum = _file_sha256(tmp)
    byte_size = tmp.stat().st_size
    blob.upload_from_filename(str(tmp))
    tmp.unlink(missing_ok=True)

    return to_store, f"gs://{bucket_name}/{object_name}", checksum, byte_size


def _load_province_points() -> list[ProvincePoint]:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM).order_by(ProvinceORM.plate)).scalars().all()

    points = [ProvincePoint(point_id=row.id, lat=float(row.lat), lon=float(row.lng)) for row in rows]
    if len(points) != 81:
        LOG.warning("openmeteo_expected_81_points got=%s", len(points))
    return points


def _build_openmeteo_client():
    try:
        import openmeteo_requests
        import requests_cache
        from retry_requests import retry
    except Exception:
        return None

    cache_path = Path(".cache") / "openmeteo"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_session = requests_cache.CachedSession(str(cache_path), expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def _as_series(values: Any, n: int) -> pd.Series:
    arr = pd.Series(values if values is not None else [], dtype="float64")
    if len(arr) >= n:
        return arr.iloc[:n].reset_index(drop=True)
    if n <= 0:
        return pd.Series([], dtype="float64")
    return pd.concat([arr, pd.Series([np.nan] * (n - len(arr)))], ignore_index=True)


def _build_canonical_frame(
    *,
    province: ProvincePoint,
    dates: list[date],
    daily_payload: dict[str, Any],
    source: str,
    run_id: str,
    ingested_at: datetime,
) -> pd.DataFrame:
    n = len(dates)
    wind_values = daily_payload.get("wind_speed_10m_max")
    if wind_values is None:
        wind_values = daily_payload.get("windspeed_10m_max")

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(pd.Series(dates), errors="coerce").dt.date,
            "point_id": province.point_id,
            "lat": province.lat,
            "lon": province.lon,
            "temp_max_c": _as_series(daily_payload.get("temperature_2m_max"), n),
            "temp_mean_c": _as_series(daily_payload.get("temperature_2m_mean"), n),
            "precip_sum_mm": _as_series(daily_payload.get("precipitation_sum"), n),
            "wind_max_ms": kmh_to_ms(_as_series(wind_values, n)),
            "et0_mm": _as_series(daily_payload.get("et0_fao_evapotranspiration"), n),
            "soil_moisture_0_10cm_m3m3": _as_series(daily_payload.get("soil_moisture_0_to_10cm_mean"), n),
            "solar_radiation_wm2": mj_per_day_to_wm2(_as_series(daily_payload.get("shortwave_radiation_sum"), n)),
            "spi_proxy_30d": np.nan,
            "source": source,
            "run_id": run_id,
            "ingested_at": ingested_at,
        }
    )
    return df


def _fetch_with_openmeteo_requests(
    *,
    url: str,
    points: list[ProvincePoint],
    source: str,
    run_id: str,
    ingested_at: datetime,
    start_date: date | None,
    end_date: date | None,
    forecast_days: int | None,
) -> pd.DataFrame:
    client = _build_openmeteo_client()
    if client is None:
        return pd.DataFrame()

    params: dict[str, Any] = {
        "latitude": [p.lat for p in points],
        "longitude": [p.lon for p in points],
        "daily": DAILY_VARIABLES,
        "timezone": TIMEZONE_NAME,
    }
    if start_date is not None and end_date is not None:
        params["start_date"] = start_date.isoformat()
        params["end_date"] = end_date.isoformat()
    if forecast_days is not None:
        params["forecast_days"] = forecast_days

    try:
        responses = client.weather_api(url, params=params)
    except Exception:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for i, response in enumerate(responses):
        if i >= len(points):
            break
        daily = response.Daily()
        start_ts = pd.to_datetime(daily.Time(), unit="s", utc=True)
        end_ts = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True)
        interval = pd.Timedelta(seconds=daily.Interval())
        dt_index = pd.date_range(start=start_ts, end=end_ts, freq=interval, inclusive="left")
        dates = dt_index.tz_convert(TIMEZONE_NAME).date.tolist()

        payload: dict[str, Any] = {}
        for idx, variable in enumerate(DAILY_VARIABLES):
            if idx < daily.VariablesLength():
                payload[variable] = daily.Variables(idx).ValuesAsNumpy()
            else:
                payload[variable] = []

        frames.append(
            _build_canonical_frame(
                province=points[i],
                dates=dates,
                daily_payload=payload,
                source=source,
                run_id=run_id,
                ingested_at=ingested_at,
            )
        )

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_with_requests_json(
    *,
    url: str,
    points: list[ProvincePoint],
    source: str,
    run_id: str,
    ingested_at: datetime,
    start_date: date | None,
    end_date: date | None,
    forecast_days: int | None,
) -> pd.DataFrame:
    params: dict[str, Any] = {
        "latitude": [p.lat for p in points],
        "longitude": [p.lon for p in points],
        "daily": DAILY_VARIABLES,
        "timezone": TIMEZONE_NAME,
    }
    if start_date is not None and end_date is not None:
        params["start_date"] = start_date.isoformat()
        params["end_date"] = end_date.isoformat()
    if forecast_days is not None:
        params["forecast_days"] = forecast_days

    response = requests.get(url, params=params, timeout=180)
    response.raise_for_status()
    payload = response.json()
    records = payload if isinstance(payload, list) else [payload]

    frames: list[pd.DataFrame] = []
    for i, item in enumerate(records):
        if i >= len(points):
            break
        daily = item.get("daily", {})
        dates = [date.fromisoformat(x) for x in daily.get("time", [])]
        frames.append(
            _build_canonical_frame(
                province=points[i],
                dates=dates,
                daily_payload=daily,
                source=source,
                run_id=run_id,
                ingested_at=ingested_at,
            )
        )

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_openmeteo_daily_frame(
    *,
    url: str,
    points: list[ProvincePoint],
    source: str,
    run_id: str,
    ingested_at: datetime,
    start_date: date | None = None,
    end_date: date | None = None,
    forecast_days: int | None = None,
) -> pd.DataFrame:
    df = _fetch_with_openmeteo_requests(
        url=url,
        points=points,
        source=source,
        run_id=run_id,
        ingested_at=ingested_at,
        start_date=start_date,
        end_date=end_date,
        forecast_days=forecast_days,
    )
    if not df.empty:
        return df
    return _fetch_with_requests_json(
        url=url,
        points=points,
        source=source,
        run_id=run_id,
        ingested_at=ingested_at,
        start_date=start_date,
        end_date=end_date,
        forecast_days=forecast_days,
    )


def _write_archive_partitions(df: pd.DataFrame) -> PartitionWriteResult:
    if df.empty:
        return PartitionWriteResult(rows_written=0, files_written=0, artifacts=[])

    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame["year"] = pd.to_datetime(frame["date"]).dt.year
    frame["month"] = pd.to_datetime(frame["date"]).dt.month

    artifacts: list[dict[str, Any]] = []
    files_written = 0
    rows_written = 0

    for (year, month), part in frame.groupby(["year", "month"], as_index=False):
        payload = part.drop(columns=["year", "month"]).copy()
        object_name = f"features/daily/openmeteo/year={int(year):04d}/month={int(month):02d}/part-0.parquet"
        merged, gcs_uri, checksum, byte_size = _upload_partition(
            df=payload,
            object_name=object_name,
            dedupe_keys=["date", "point_id"],
        )
        files_written += 1
        rows_written += len(payload)
        artifacts.append(
            {
                "artifact_type": "feature_daily_parquet",
                "gcs_uri": gcs_uri,
                "start_date": payload["date"].min(),
                "end_date": payload["date"].max(),
                "row_count": len(merged),
                "checksum_sha256": checksum,
                "byte_size": byte_size,
            }
        )

    return PartitionWriteResult(rows_written=rows_written, files_written=files_written, artifacts=artifacts)


def _write_forecast_partitions(df: pd.DataFrame) -> PartitionWriteResult:
    if df.empty:
        return PartitionWriteResult(rows_written=0, files_written=0, artifacts=[])

    frame = df.copy()
    frame["target_date"] = pd.to_datetime(frame["target_date"]).dt.date

    artifacts: list[dict[str, Any]] = []
    files_written = 0

    for target_date, part in frame.groupby("target_date", as_index=False):
        dt = pd.Timestamp(target_date).date()
        object_name = (
            f"features/forecast/openmeteo/target_year={dt.year:04d}/"
            f"target_month={dt.month:02d}/target_day={dt.day:02d}/part-0.parquet"
        )
        uploaded, gcs_uri, checksum, byte_size = _upload_partition(
            df=part.copy(),
            object_name=object_name,
            dedupe_keys=None,
        )
        files_written += 1
        artifacts.append(
            {
                "artifact_type": "forecast_daily_parquet",
                "gcs_uri": gcs_uri,
                "start_date": dt,
                "end_date": dt,
                "row_count": len(uploaded),
                "checksum_sha256": checksum,
                "byte_size": byte_size,
            }
        )

    return PartitionWriteResult(rows_written=len(frame), files_written=files_written, artifacts=artifacts)


def _save_artifacts(job_id: str, artifacts: list[dict[str, Any]]) -> None:
    if not artifacts:
        return
    with SessionLocal() as db:
        for item in artifacts:
            db.add(
                OpenMeteoArtifactORM(
                    job_id=job_id,
                    artifact_type=item["artifact_type"],
                    gcs_uri=item["gcs_uri"],
                    start_date=item["start_date"],
                    end_date=item["end_date"],
                    row_count=int(item["row_count"]),
                    checksum_sha256=item["checksum_sha256"],
                    byte_size=int(item["byte_size"]),
                )
            )
        db.commit()


def _iter_year_ranges(start_date: date, end_date: date) -> list[tuple[int, date, date]]:
    out: list[tuple[int, date, date]] = []
    for year in range(start_date.year, end_date.year + 1):
        y_start = date(year, 1, 1)
        y_end = date(year, 12, 31)
        out.append((year, max(start_date, y_start), min(end_date, y_end)))
    return out


def _update_job(
    job_id: str,
    *,
    status: str | None = None,
    rows_written: int | None = None,
    files_written: int | None = None,
    progress: dict[str, Any] | None = None,
    error: str | None = None,
    started: datetime | None = None,
    finished: datetime | None = None,
    duration_seconds: float | None = None,
) -> None:
    with SessionLocal() as db:
        row = db.get(OpenMeteoJobORM, job_id)
        if row is None:
            return
        if status is not None:
            row.status = status
        if rows_written is not None:
            row.rows_written = rows_written
        if files_written is not None:
            row.files_written = files_written
        if progress is not None:
            row.progress_json = json.dumps(progress)
        if error is not None:
            row.error = error
        if started is not None:
            row.started_at = started
        if finished is not None:
            row.finished_at = finished
        if duration_seconds is not None:
            row.duration_seconds = duration_seconds
        db.commit()


def _archive_year_worker(
    *,
    year: int,
    window_start: date,
    window_end: date,
    points: list[ProvincePoint],
    run_id: str,
    ingested_at: datetime,
) -> PartitionWriteResult:
    fetch_start = window_start - timedelta(days=29)
    raw = fetch_openmeteo_daily_frame(
        url=ARCHIVE_URL,
        points=points,
        source="open_meteo_archive",
        run_id=run_id,
        ingested_at=ingested_at,
        start_date=fetch_start,
        end_date=window_end,
    )
    if raw.empty:
        return PartitionWriteResult(rows_written=0, files_written=0, artifacts=[])

    enriched = compute_spi_proxy_30d(raw)
    filtered = enriched[(enriched["date"] >= window_start) & (enriched["date"] <= window_end)].copy()
    result = _write_archive_partitions(filtered)
    LOG.info(
        "openmeteo_year_done year=%s rows=%s files=%s", year, result.rows_written, result.files_written
    )
    return result


def _run_backfill(job_id: str, *, start_date: date, end_date: date, run_id: str, concurrency: int) -> tuple[dict[str, Any], int, int, list[dict[str, Any]]]:
    points = _load_province_points()
    years = _iter_year_ranges(start_date, end_date)
    workers = max(1, min(concurrency, len(years), 10))
    ingested_at = datetime.now(timezone.utc)

    progress: dict[str, Any] = {
        "years_total": len(years),
        "years_success": 0,
        "years_failed": 0,
        "failed_years": [],
        "request": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "concurrency": workers,
        },
    }
    _update_job(job_id, progress=progress)

    rows_written = 0
    files_written = 0
    artifacts: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {
            pool.submit(
                _archive_year_worker,
                year=year,
                window_start=window_start,
                window_end=window_end,
                points=points,
                run_id=run_id,
                ingested_at=ingested_at,
            ): year
            for year, window_start, window_end in years
        }

        for future in as_completed(future_map):
            year = future_map[future]
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001
                progress["years_failed"] = int(progress["years_failed"]) + 1
                progress["failed_years"].append(str(year))
                LOG.exception("openmeteo_backfill_year_failed year=%s error=%s", year, str(exc))
            else:
                progress["years_success"] = int(progress["years_success"]) + 1
                rows_written += result.rows_written
                files_written += result.files_written
                artifacts.extend(result.artifacts)
            _update_job(
                job_id,
                rows_written=rows_written,
                files_written=files_written,
                progress=progress,
            )

    return progress, rows_written, files_written, artifacts


def _run_daily(job_id: str, *, target_date: date, run_id: str) -> tuple[dict[str, Any], int, int, list[dict[str, Any]]]:
    points = _load_province_points()
    ingested_at = datetime.now(timezone.utc)
    fetch_start = target_date - timedelta(days=29)

    raw = fetch_openmeteo_daily_frame(
        url=ARCHIVE_URL,
        points=points,
        source="open_meteo_archive",
        run_id=run_id,
        ingested_at=ingested_at,
        start_date=fetch_start,
        end_date=target_date,
    )
    enriched = compute_spi_proxy_30d(raw)
    filtered = enriched[enriched["date"] == target_date].copy()
    write = _write_archive_partitions(filtered)

    progress = {
        "target_date": target_date.isoformat(),
        "rows_written": write.rows_written,
        "files_written": write.files_written,
    }
    return progress, write.rows_written, write.files_written, write.artifacts


def _run_forecast(job_id: str, *, forecast_days: int, run_id: str) -> tuple[dict[str, Any], int, int, list[dict[str, Any]]]:
    points = _load_province_points()
    run_ts = datetime.now(timezone.utc)

    forecast = fetch_openmeteo_daily_frame(
        url=FORECAST_URL,
        points=points,
        source="open_meteo_forecast",
        run_id=run_id,
        ingested_at=run_ts,
        forecast_days=forecast_days,
    )
    forecast["run_timestamp_utc"] = run_ts
    forecast["run_date_utc"] = run_ts.date()
    forecast["target_date"] = pd.to_datetime(forecast["date"]).dt.date
    forecast["horizon_days"] = (
        pd.to_datetime(forecast["target_date"]) - pd.Timestamp(run_ts.date())
    ).dt.days.astype("int64")
    forecast["model"] = MODEL_NAME
    forecast["timezone"] = TIMEZONE_NAME

    write = _write_forecast_partitions(forecast)
    progress = {
        "forecast_days": forecast_days,
        "targets_written": write.files_written,
        "run_timestamp_utc": run_ts.isoformat(),
    }
    return progress, write.rows_written, write.files_written, write.artifacts


def process_openmeteo_job(job_id: str) -> None:
    started_at = datetime.now(timezone.utc)

    with SessionLocal() as db:
        job = db.get(OpenMeteoJobORM, job_id)
        if job is None:
            return
        job.status = "running"
        job.started_at = started_at
        db.commit()
        job_type = job.job_type
        start_date = job.start_date
        end_date = job.end_date
        run_id = job.run_id
        progress_payload: dict[str, Any] = {}
        if job.progress_json:
            try:
                progress_payload = json.loads(job.progress_json)
            except Exception:  # noqa: BLE001
                progress_payload = {}

    status = "success"
    error: str | None = None
    rows_written = 0
    files_written = 0
    artifacts: list[dict[str, Any]] = []
    progress: dict[str, Any] = progress_payload

    try:
        if job_type == JOB_TYPE_BACKFILL:
            concurrency = int(progress_payload.get("request", {}).get("concurrency", 10))
            progress, rows_written, files_written, artifacts = _run_backfill(
                job_id,
                start_date=start_date,
                end_date=end_date,
                run_id=run_id,
                concurrency=concurrency,
            )
            if progress.get("years_failed"):
                status = "failed"
        elif job_type == JOB_TYPE_DAILY:
            target_date_raw = progress_payload.get("request", {}).get("target_date", start_date.isoformat())
            target_date = date.fromisoformat(target_date_raw)
            progress, rows_written, files_written, artifacts = _run_daily(
                job_id,
                target_date=target_date,
                run_id=run_id,
            )
        elif job_type == JOB_TYPE_FORECAST:
            forecast_days = int(progress_payload.get("request", {}).get("forecast_days", 16))
            progress, rows_written, files_written, artifacts = _run_forecast(
                job_id,
                forecast_days=forecast_days,
                run_id=run_id,
            )
        else:
            raise RuntimeError(f"Unsupported openmeteo job type: {job_type}")

        _save_artifacts(job_id, artifacts)

    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error = str(exc)
        LOG.exception("openmeteo_job_failed job_id=%s type=%s error=%s", job_id, job_type, error)

    finished_at = datetime.now(timezone.utc)
    _update_job(
        job_id,
        status=status,
        rows_written=rows_written,
        files_written=files_written,
        progress=progress,
        error=error,
        finished=finished_at,
        duration_seconds=(finished_at - started_at).total_seconds(),
    )


def start_openmeteo_background_job(job_id: str) -> None:
    thread = threading.Thread(target=process_openmeteo_job, args=(job_id,), daemon=False)
    thread.start()


def _create_job(
    *,
    job_type: str,
    start_date: date,
    end_date: date,
    request_payload: dict[str, Any],
) -> tuple[str, bool]:
    signature = sha256(
        json.dumps(
            {
                "job_type": job_type,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "request": request_payload,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    with SessionLocal() as db:
        existing = db.execute(
            select(OpenMeteoJobORM)
            .where(
                OpenMeteoJobORM.request_signature == signature,
                OpenMeteoJobORM.status.in_(["queued", "running", "success"]),
            )
            .order_by(desc(OpenMeteoJobORM.created_at))
            .limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing.job_id, True

        job_id = str(uuid4())
        run_id = str(uuid4())
        db.add(
            OpenMeteoJobORM(
                job_id=job_id,
                request_signature=signature,
                job_type=job_type,
                status="queued",
                start_date=start_date,
                end_date=end_date,
                rows_written=0,
                files_written=0,
                run_id=run_id,
                progress_json=json.dumps({"request": request_payload}),
                error=None,
            )
        )
        db.commit()

    start_openmeteo_background_job(job_id)
    return job_id, False


def submit_openmeteo_backfill(*, start_date: date, end_date: date, concurrency: int = 10) -> tuple[str, bool]:
    return _create_job(
        job_type=JOB_TYPE_BACKFILL,
        start_date=start_date,
        end_date=end_date,
        request_payload={"concurrency": max(1, min(int(concurrency), 10))},
    )


def submit_openmeteo_daily(*, target_date: date | None = None) -> tuple[str, bool, date]:
    target = target_date or (datetime.now(timezone.utc).date() - timedelta(days=1))
    job_id, dedup = _create_job(
        job_type=JOB_TYPE_DAILY,
        start_date=target,
        end_date=target,
        request_payload={"target_date": target.isoformat()},
    )
    return job_id, dedup, target


def submit_openmeteo_forecast(*, forecast_days: int = 16) -> tuple[str, bool]:
    days = max(1, min(int(forecast_days), 16))
    start = datetime.now(timezone.utc).date()
    end = start + timedelta(days=days - 1)
    return _create_job(
        job_type=JOB_TYPE_FORECAST,
        start_date=start,
        end_date=end,
        request_payload={"forecast_days": days},
    )


def get_openmeteo_job(job_id: str) -> OpenMeteoJobORM | None:
    with SessionLocal() as db:
        return db.get(OpenMeteoJobORM, job_id)
