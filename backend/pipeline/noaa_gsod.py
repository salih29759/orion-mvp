from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from io import StringIO
import logging
from pathlib import Path
import tempfile
import threading
import time
from uuid import uuid4

from google.cloud import storage
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.errors import ApiError
from app.orm import NoaaBackfillProgressORM, ProvinceORM

LOG = logging.getLogger("orion.noaa.gsod")

ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
GSOD_PRIMARY_TEMPLATE = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year:04d}/{station_id}.csv"
GSOD_FALLBACK_URL = "https://www.ncei.noaa.gov/access/services/data/v1"

NOAA_SOURCE = "noaa_gsod"
NOAA_TYPE = "noaa_gsod_backfill"

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_COMPLETED_WITH_FAILURES = "completed_with_failures"
STATUS_FAILED = "failed"

FEATURE_COLUMNS = [
    "date",
    "station_id",
    "province_id",
    "lat",
    "lon",
    "temp_mean_c",
    "temp_max_c",
    "temp_min_c",
    "precip_mm",
    "wind_mean_ms",
    "wind_gust_ms",
    "heat_extreme",
    "frost_event",
    "heavy_rain",
    "strong_wind",
    "source",
    "run_id",
    "ingested_at",
]

FEATURE_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("station_id", pa.string()),
        pa.field("province_id", pa.string()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("temp_mean_c", pa.float64()),
        pa.field("temp_max_c", pa.float64()),
        pa.field("temp_min_c", pa.float64()),
        pa.field("precip_mm", pa.float64()),
        pa.field("wind_mean_ms", pa.float64()),
        pa.field("wind_gust_ms", pa.float64()),
        pa.field("heat_extreme", pa.bool_()),
        pa.field("frost_event", pa.bool_()),
        pa.field("heavy_rain", pa.bool_()),
        pa.field("strong_wind", pa.bool_()),
        pa.field("source", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def _storage_client() -> storage.Client:
    return storage.Client()


def validate_noaa_runtime() -> list[str]:
    missing: list[str] = []
    if not settings.era5_gcs_bucket:
        missing.append("ERA5_GCS_BUCKET")
    return missing


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _month_end(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1) - timedelta(days=1)
    return date(value.year, value.month + 1, 1) - timedelta(days=1)


def iter_month_starts(start_value: date, end_value: date) -> list[date]:
    cur = _month_start(start_value)
    end_month = _month_start(end_value)
    out: list[date] = []
    while cur <= end_month:
        out.append(cur)
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def clamp_end_to_yesterday(requested_end: date) -> date:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if requested_end <= yesterday:
        return requested_end
    return yesterday


def month_object_name(month_value: date) -> str:
    return f"features/daily/noaa_gsod/year={month_value.year:04d}/month={month_value.month:02d}/part-0.parquet"


def metadata_object_name() -> str:
    return "metadata/noaa/isd-history-tu.csv"


def month_exists(*, bucket_name: str, month_value: date) -> bool:
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(month_object_name(month_value)).exists()


def _download_csv(url: str, *, timeout_sec: int = 60) -> str:
    res = requests.get(url, timeout=timeout_sec)
    res.raise_for_status()
    return res.text


def download_turkey_station_list() -> pd.DataFrame:
    text = _download_csv(ISD_HISTORY_URL, timeout_sec=90)
    frame = pd.read_csv(StringIO(text), dtype=str)
    frame.columns = [str(c).strip() for c in frame.columns]

    required = {"USAF", "WBAN", "CTRY", "LAT", "LON"}
    if not required.issubset(set(frame.columns)):
        raise RuntimeError("NOAA isd-history.csv is missing required columns")

    out = frame.loc[frame["CTRY"].fillna("").str.strip() == "TU"].copy()
    out["lat"] = pd.to_numeric(out["LAT"], errors="coerce")
    out["lon"] = pd.to_numeric(out["LON"], errors="coerce")
    out = out.loc[out["lat"].notna() & out["lon"].notna()].copy()

    out["usaf"] = out["USAF"].fillna("").str.strip().str.zfill(6)
    out["wban"] = out["WBAN"].fillna("").str.strip().str.zfill(5)
    out = out.loc[(out["usaf"].str.len() == 6) & (out["wban"].str.len() == 5)].copy()
    out["station_id"] = out["usaf"] + out["wban"]

    out["station_name"] = out.get("STATION NAME", pd.Series([""] * len(out))).fillna("").astype(str)
    out["icao"] = out.get("ICAO", pd.Series([""] * len(out))).fillna("").astype(str)
    out["state"] = out.get("STATE", pd.Series([""] * len(out))).fillna("").astype(str)
    out["elev_m"] = pd.to_numeric(out.get("ELEV(M)", pd.Series([None] * len(out))), errors="coerce")
    out["begin"] = out.get("BEGIN", pd.Series([""] * len(out))).fillna("").astype(str)
    out["end"] = out.get("END", pd.Series([""] * len(out))).fillna("").astype(str)

    out = out[
        [
            "station_id",
            "usaf",
            "wban",
            "station_name",
            "state",
            "icao",
            "lat",
            "lon",
            "elev_m",
            "begin",
            "end",
        ]
    ].drop_duplicates(subset=["station_id"])

    out = out.sort_values("station_id").reset_index(drop=True)
    return out


def _upload_station_metadata(*, stations: pd.DataFrame, bucket_name: str) -> str:
    csv_text = stations.to_csv(index=False)
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(metadata_object_name())
    blob.upload_from_string(csv_text, content_type="text/csv")
    return f"gs://{bucket_name}/{metadata_object_name()}"


def _load_province_centroids() -> pd.DataFrame:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM.id, ProvinceORM.lat, ProvinceORM.lng).order_by(ProvinceORM.id)).all()
    if not rows:
        raise RuntimeError("No provinces found in database")
    return pd.DataFrame(rows, columns=["province_id", "lat", "lon"])


def map_stations_to_provinces(stations: pd.DataFrame, provinces: pd.DataFrame) -> pd.DataFrame:
    if stations.empty:
        return stations.assign(province_id=pd.Series(dtype=str))

    prov_lats = provinces["lat"].to_numpy(dtype=np.float64)
    prov_lons = provinces["lon"].to_numpy(dtype=np.float64)
    prov_ids = provinces["province_id"].astype(str).to_numpy(dtype=object)

    mappings: list[str] = []
    for station in stations.itertuples(index=False):
        dist = np.abs(prov_lats - float(station.lat)) + np.abs(prov_lons - float(station.lon))
        idx = int(np.argmin(dist))
        mappings.append(str(prov_ids[idx]))

    out = stations.copy()
    out["province_id"] = mappings
    return out


def _clean_number(raw: object, *, missing_sentinel: float) -> float | None:
    if raw is None:
        return None
    try:
        value = float(str(raw).strip())
    except Exception:  # noqa: BLE001
        return None
    if abs(value - missing_sentinel) < 1e-9:
        return None
    return value


def _f_to_c(value_f: float | None) -> float | None:
    if value_f is None:
        return None
    return (value_f - 32.0) * (5.0 / 9.0)


def _in_to_mm(value_in: float | None) -> float | None:
    if value_in is None:
        return None
    return value_in * 25.4


def _knots_to_ms(value_knots: float | None) -> float | None:
    if value_knots is None:
        return None
    return value_knots * 0.514444


def _normalize_gsod_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.columns = [str(c).strip().upper() for c in out.columns]
    return out


def _primary_station_year_url(station_id: str, year: int) -> str:
    return GSOD_PRIMARY_TEMPLATE.format(year=year, station_id=station_id)


def _fallback_station_data(*, station_id: str, start_date: date, end_date: date) -> pd.DataFrame:
    params = {
        "dataset": "global-summary-of-the-day",
        "stations": station_id,
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "format": "json",
    }
    res = requests.get(GSOD_FALLBACK_URL, params=params, timeout=60)
    res.raise_for_status()

    payload = res.json()
    if not isinstance(payload, list):
        return pd.DataFrame()
    if not payload:
        return pd.DataFrame()
    return _normalize_gsod_frame(pd.DataFrame(payload))


def _fetch_station_month_frame(*, station_id: str, start_date: date, end_date: date) -> pd.DataFrame:
    errors: list[str] = []
    year = start_date.year

    try:
        primary_url = _primary_station_year_url(station_id, year)
        res = requests.get(primary_url, timeout=60)
        if res.status_code >= 400:
            raise RuntimeError(f"primary_http_{res.status_code}")
        primary_df = _normalize_gsod_frame(pd.read_csv(StringIO(res.text), dtype=str))
        if "DATE" not in primary_df.columns:
            raise RuntimeError("primary_missing_date_column")
        primary_df["DATE"] = pd.to_datetime(primary_df["DATE"], errors="coerce").dt.date
        primary_df = primary_df.loc[primary_df["DATE"].notna()].copy()
        primary_df = primary_df.loc[(primary_df["DATE"] >= start_date) & (primary_df["DATE"] <= end_date)].copy()
        return primary_df
    except Exception as exc:  # noqa: BLE001
        errors.append(f"primary={exc}")

    try:
        fallback_df = _fallback_station_data(station_id=station_id, start_date=start_date, end_date=end_date)
        if fallback_df.empty:
            return fallback_df
        if "DATE" not in fallback_df.columns:
            raise RuntimeError("fallback_missing_date_column")
        fallback_df["DATE"] = pd.to_datetime(fallback_df["DATE"], errors="coerce").dt.date
        fallback_df = fallback_df.loc[fallback_df["DATE"].notna()].copy()
        fallback_df = fallback_df.loc[(fallback_df["DATE"] >= start_date) & (fallback_df["DATE"] <= end_date)].copy()
        return fallback_df
    except Exception as exc:  # noqa: BLE001
        errors.append(f"fallback={exc}")
        raise RuntimeError("; ".join(errors)) from exc


def empty_feature_frame() -> pd.DataFrame:
    frame = pd.DataFrame(columns=FEATURE_COLUMNS)
    frame["date"] = pd.Series(dtype="object")
    frame["station_id"] = pd.Series(dtype="string")
    frame["province_id"] = pd.Series(dtype="string")
    frame["lat"] = pd.Series(dtype="float64")
    frame["lon"] = pd.Series(dtype="float64")
    frame["temp_mean_c"] = pd.Series(dtype="float64")
    frame["temp_max_c"] = pd.Series(dtype="float64")
    frame["temp_min_c"] = pd.Series(dtype="float64")
    frame["precip_mm"] = pd.Series(dtype="float64")
    frame["wind_mean_ms"] = pd.Series(dtype="float64")
    frame["wind_gust_ms"] = pd.Series(dtype="float64")
    frame["heat_extreme"] = pd.Series(dtype="bool")
    frame["frost_event"] = pd.Series(dtype="bool")
    frame["heavy_rain"] = pd.Series(dtype="bool")
    frame["strong_wind"] = pd.Series(dtype="bool")
    frame["source"] = pd.Series(dtype="string")
    frame["run_id"] = pd.Series(dtype="string")
    frame["ingested_at"] = pd.Series(dtype="datetime64[ns, UTC]")
    return frame


def _to_feature_rows(
    *,
    station_id: str,
    province_id: str,
    station_lat: float,
    station_lon: float,
    daily: pd.DataFrame,
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[dict], int]:
    rows: list[dict] = []
    proxy_used = 0

    for rec in daily.to_dict("records"):
        day = rec.get("DATE")
        if not isinstance(day, date):
            continue

        temp_mean_c = _f_to_c(_clean_number(rec.get("TEMP"), missing_sentinel=9999.9))
        temp_max_c = _f_to_c(_clean_number(rec.get("MAX"), missing_sentinel=9999.9))
        temp_min_c = _f_to_c(_clean_number(rec.get("MIN"), missing_sentinel=9999.9))
        precip_mm = _in_to_mm(_clean_number(rec.get("PRCP"), missing_sentinel=99.99))
        wind_mean_ms = _knots_to_ms(_clean_number(rec.get("WDSP"), missing_sentinel=999.9))
        wind_gust_ms = _knots_to_ms(_clean_number(rec.get("GUST"), missing_sentinel=999.9))

        heat_extreme = (temp_max_c is not None) and (temp_max_c >= 40.0)
        frost_event = (temp_min_c is not None) and (temp_min_c <= 0.0)
        heavy_rain = (precip_mm is not None) and (precip_mm >= 50.0)

        if wind_gust_ms is not None:
            strong_wind = wind_gust_ms >= 20.0
        elif wind_mean_ms is not None:
            strong_wind = wind_mean_ms >= 15.0
            proxy_used += 1
        else:
            strong_wind = False

        rows.append(
            {
                "date": day,
                "station_id": station_id,
                "province_id": province_id,
                "lat": float(station_lat),
                "lon": float(station_lon),
                "temp_mean_c": temp_mean_c,
                "temp_max_c": temp_max_c,
                "temp_min_c": temp_min_c,
                "precip_mm": precip_mm,
                "wind_mean_ms": wind_mean_ms,
                "wind_gust_ms": wind_gust_ms,
                "heat_extreme": bool(heat_extreme),
                "frost_event": bool(frost_event),
                "heavy_rain": bool(heavy_rain),
                "strong_wind": bool(strong_wind),
                "source": NOAA_SOURCE,
                "run_id": run_id,
                "ingested_at": ingested_at,
            }
        )

    return rows, proxy_used


def _write_month_parquet(*, bucket_name: str, month_value: date, frame: pd.DataFrame) -> str:
    if frame.empty:
        frame = empty_feature_frame()

    frame = frame[FEATURE_COLUMNS].copy()
    frame["ingested_at"] = pd.to_datetime(frame["ingested_at"], utc=True, errors="coerce")

    local_path = Path(tempfile.gettempdir()) / f"orion_noaa_{month_value.year:04d}_{month_value.month:02d}.parquet"
    table = pa.Table.from_pandas(frame, schema=FEATURE_SCHEMA, preserve_index=False, safe=False)
    pq.write_table(table, local_path)

    client = _storage_client()
    bucket = client.bucket(bucket_name)
    obj = month_object_name(month_value)
    bucket.blob(obj).upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{obj}"


def _insert_pending_rows(*, run_id: str, months: list[date], now: datetime) -> None:
    with SessionLocal() as db:
        for month_value in months:
            db.add(
                NoaaBackfillProgressORM(
                    run_id=run_id,
                    month=month_value,
                    status=STATUS_PENDING,
                    rows_written=0,
                    stations_total=0,
                    stations_success=0,
                    stations_failed=0,
                    strong_wind_proxy_used=0,
                    updated_at=now,
                )
            )
        db.commit()


def _update_row(
    *,
    run_id: str,
    month_value: date,
    status: str,
    rows_written: int | None = None,
    stations_total: int | None = None,
    stations_success: int | None = None,
    stations_failed: int | None = None,
    strong_wind_proxy_used: int | None = None,
    error_msg: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    duration_sec: float | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        row = db.execute(
            select(NoaaBackfillProgressORM).where(
                NoaaBackfillProgressORM.run_id == run_id,
                NoaaBackfillProgressORM.month == month_value,
            )
        ).scalar_one_or_none()
        if row is None:
            return
        row.status = status
        row.updated_at = now
        if rows_written is not None:
            row.rows_written = int(rows_written)
        if stations_total is not None:
            row.stations_total = int(stations_total)
        if stations_success is not None:
            row.stations_success = int(stations_success)
        if stations_failed is not None:
            row.stations_failed = int(stations_failed)
        if strong_wind_proxy_used is not None:
            row.strong_wind_proxy_used = int(strong_wind_proxy_used)
        if error_msg is not None:
            row.error_msg = str(error_msg)[:2000]
        if started_at is not None:
            row.started_at = started_at
        if completed_at is not None:
            row.completed_at = completed_at
        if duration_sec is not None:
            row.duration_sec = float(duration_sec)
        db.commit()


def _process_station(
    *,
    station_row: pd.Series,
    start_date: date,
    end_date: date,
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[dict], int]:
    station_id = str(station_row["station_id"])
    daily = _fetch_station_month_frame(station_id=station_id, start_date=start_date, end_date=end_date)
    if daily.empty:
        return [], 0

    return _to_feature_rows(
        station_id=station_id,
        province_id=str(station_row["province_id"]),
        station_lat=float(station_row["lat"]),
        station_lon=float(station_row["lon"]),
        daily=daily,
        run_id=run_id,
        ingested_at=ingested_at,
    )


def _process_month(
    *,
    run_id: str,
    month_value: date,
    requested_start: date,
    requested_end: date,
    stations: pd.DataFrame,
    concurrency: int,
    force: bool,
) -> None:
    bucket = settings.era5_gcs_bucket
    if not bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    started = datetime.now(timezone.utc)
    started_monotonic = time.time()
    _update_row(
        run_id=run_id,
        month_value=month_value,
        status=STATUS_RUNNING,
        started_at=started,
        error_msg=None,
    )

    try:
        if month_exists(bucket_name=bucket, month_value=month_value) and not force:
            _update_row(
                run_id=run_id,
                month_value=month_value,
                status=STATUS_COMPLETED,
                rows_written=0,
                stations_total=0,
                stations_success=0,
                stations_failed=0,
                strong_wind_proxy_used=0,
                completed_at=datetime.now(timezone.utc),
                duration_sec=time.time() - started_monotonic,
            )
            LOG.info("noaa_month_skip_existing run_id=%s month=%s", run_id, month_value.strftime("%Y-%m"))
            return

        month_start = max(month_value, requested_start)
        month_end = min(_month_end(month_value), requested_end)
        if month_end < month_start:
            _update_row(
                run_id=run_id,
                month_value=month_value,
                status=STATUS_COMPLETED,
                rows_written=0,
                stations_total=0,
                stations_success=0,
                stations_failed=0,
                strong_wind_proxy_used=0,
                completed_at=datetime.now(timezone.utc),
                duration_sec=time.time() - started_monotonic,
            )
            return

        stations_total = int(len(stations.index))
        stations_success = 0
        stations_failed = 0
        proxy_used_total = 0
        all_rows: list[dict] = []
        station_errors: list[str] = []
        ingested_at = datetime.now(timezone.utc)

        worker_count = max(1, min(int(concurrency), 8))
        with ThreadPoolExecutor(max_workers=worker_count) as ex:
            futures = {
                ex.submit(
                    _process_station,
                    station_row=row,
                    start_date=month_start,
                    end_date=month_end,
                    run_id=run_id,
                    ingested_at=ingested_at,
                ): str(row["station_id"])
                for _, row in stations.iterrows()
            }
            for future in as_completed(futures):
                station_id = futures[future]
                try:
                    rows, proxy_used = future.result()
                    stations_success += 1
                    proxy_used_total += int(proxy_used)
                    if rows:
                        all_rows.extend(rows)
                except Exception as exc:  # noqa: BLE001
                    stations_failed += 1
                    station_errors.append(f"{station_id}: {exc}")

        frame = pd.DataFrame(all_rows, columns=FEATURE_COLUMNS) if all_rows else empty_feature_frame()
        output_uri = _write_month_parquet(bucket_name=bucket, month_value=month_value, frame=frame)
        rows_written = int(len(frame.index))

        row_status = STATUS_COMPLETED if stations_failed == 0 else STATUS_COMPLETED_WITH_FAILURES
        error_msg = None
        if station_errors:
            error_msg = "; ".join(station_errors[:5])

        _update_row(
            run_id=run_id,
            month_value=month_value,
            status=row_status,
            rows_written=rows_written,
            stations_total=stations_total,
            stations_success=stations_success,
            stations_failed=stations_failed,
            strong_wind_proxy_used=proxy_used_total,
            error_msg=error_msg,
            completed_at=datetime.now(timezone.utc),
            duration_sec=time.time() - started_monotonic,
        )
        LOG.info(
            "noaa_month_complete run_id=%s month=%s rows=%s stations_success=%s stations_failed=%s proxy_used=%s gcs=%s",
            run_id,
            month_value.strftime("%Y-%m"),
            rows_written,
            stations_success,
            stations_failed,
            proxy_used_total,
            output_uri,
        )
    except Exception as exc:  # noqa: BLE001
        _update_row(
            run_id=run_id,
            month_value=month_value,
            status=STATUS_FAILED,
            error_msg=str(exc),
            completed_at=datetime.now(timezone.utc),
            duration_sec=time.time() - started_monotonic,
        )
        LOG.exception("noaa_month_failed run_id=%s month=%s error=%s", run_id, month_value.strftime("%Y-%m"), str(exc))


def _run_backfill(
    *,
    run_id: str,
    months: list[date],
    requested_start: date,
    requested_end: date,
    concurrency: int,
    force: bool,
) -> None:
    bucket = settings.era5_gcs_bucket
    if not bucket:
        LOG.error("noaa_backfill_missing_bucket run_id=%s", run_id)
        return

    try:
        stations = download_turkey_station_list()
        if stations.empty:
            raise RuntimeError("Turkey station list is empty")

        provinces = _load_province_centroids()
        stations = map_stations_to_provinces(stations, provinces)
        _upload_station_metadata(stations=stations, bucket_name=bucket)

    except Exception as exc:  # noqa: BLE001
        LOG.exception("noaa_backfill_init_failed run_id=%s error=%s", run_id, str(exc))
        for month_value in months:
            _update_row(
                run_id=run_id,
                month_value=month_value,
                status=STATUS_FAILED,
                error_msg=f"init_failed: {exc}",
                completed_at=datetime.now(timezone.utc),
            )
        return

    for month_value in months:
        _process_month(
            run_id=run_id,
            month_value=month_value,
            requested_start=requested_start,
            requested_end=requested_end,
            stations=stations,
            concurrency=concurrency,
            force=force,
        )


def create_backfill_run(*, start: date, end: date, concurrency: int, force: bool) -> dict:
    missing = validate_noaa_runtime()
    if missing:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message=f"Missing env vars: {', '.join(missing)}")

    if start > end:
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message="start must be <= end")

    effective_end = clamp_end_to_yesterday(end)
    if effective_end < start:
        raise ApiError(
            status_code=422,
            error_code="VALIDATION_ERROR",
            message="Requested range is fully in the future",
            details={"start": start.isoformat(), "end": end.isoformat(), "effective_end": effective_end.isoformat()},
        )

    months = iter_month_starts(start, effective_end)
    run_id = f"noaa_{uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)

    _insert_pending_rows(run_id=run_id, months=months, now=now)

    thread = threading.Thread(
        target=_run_backfill,
        kwargs={
            "run_id": run_id,
            "months": months,
            "requested_start": start,
            "requested_end": effective_end,
            "concurrency": max(1, min(int(concurrency), 8)),
            "force": bool(force),
        },
        daemon=False,
    )
    thread.start()

    return {
        "run_id": run_id,
        "status": "queued",
        "type": NOAA_TYPE,
        "created_at": now,
        "total_months": len(months),
        "effective_start": start,
        "effective_end": effective_end,
        "progress": {
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "effective_end": effective_end.isoformat(),
            "months_total": len(months),
            "completed": 0,
            "failed": 0,
            "running": 0,
            "pending": len(months),
            "rows_written": 0,
            "stations_total": 0,
            "stations_success": 0,
            "stations_failed": 0,
            "strong_wind_proxy_used": 0,
        },
    }


def _status_for_rows(run_id: str, rows: list[NoaaBackfillProgressORM]) -> dict:
    total = len(rows)
    completed = sum(1 for r in rows if r.status in {STATUS_COMPLETED, STATUS_COMPLETED_WITH_FAILURES})
    failed = sum(1 for r in rows if r.status == STATUS_FAILED)
    running = sum(1 for r in rows if r.status == STATUS_RUNNING)
    pending = sum(1 for r in rows if r.status == STATUS_PENDING)
    partial = sum(1 for r in rows if r.status == STATUS_COMPLETED_WITH_FAILURES)

    done = completed + failed
    percent_done = round((done / total) * 100.0, 2) if total else 0.0
    last_updated = max((r.updated_at for r in rows), default=None)
    recent_errors = [
        {"month": r.month.strftime("%Y-%m"), "error": str(r.error_msg)}
        for r in sorted(rows, key=lambda x: x.updated_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        if r.error_msg
    ][:5]

    rows_written = sum(int(r.rows_written or 0) for r in rows)
    stations_total = sum(int(r.stations_total or 0) for r in rows)
    stations_success = sum(int(r.stations_success or 0) for r in rows)
    stations_failed = sum(int(r.stations_failed or 0) for r in rows)
    proxy_used = sum(int(r.strong_wind_proxy_used or 0) for r in rows)

    if total == 0:
        status = "idle"
    elif running > 0:
        status = "running"
    elif pending > 0:
        status = "queued"
    elif failed > 0 or partial > 0:
        status = "completed_with_failures"
    else:
        status = "completed"

    return {
        "run_id": run_id,
        "total_months": total,
        "completed": completed,
        "failed": failed,
        "running": running,
        "pending": pending,
        "percent_done": percent_done,
        "rows_written": rows_written,
        "stations_total": stations_total,
        "stations_success": stations_success,
        "stations_failed": stations_failed,
        "strong_wind_proxy_used": proxy_used,
        "last_updated": last_updated.isoformat() if last_updated else None,
        "recent_errors": recent_errors,
        "status": status,
    }


def get_latest_status() -> dict:
    with SessionLocal() as db:
        latest_run_id = db.execute(
            select(NoaaBackfillProgressORM.run_id)
            .order_by(desc(NoaaBackfillProgressORM.updated_at))
            .limit(1)
        ).scalar_one_or_none()

        if latest_run_id is None:
            return {
                "run_id": None,
                "total_months": 0,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "pending": 0,
                "percent_done": 0.0,
                "rows_written": 0,
                "stations_total": 0,
                "stations_success": 0,
                "stations_failed": 0,
                "strong_wind_proxy_used": 0,
                "last_updated": None,
                "recent_errors": [],
                "status": "idle",
            }

        rows = db.execute(
            select(NoaaBackfillProgressORM)
            .where(NoaaBackfillProgressORM.run_id == latest_run_id)
            .order_by(NoaaBackfillProgressORM.month)
        ).scalars().all()

    return _status_for_rows(str(latest_run_id), rows)


def run_daily_update() -> dict:
    target = datetime.now(timezone.utc).date() - timedelta(days=1)
    out = create_backfill_run(start=target, end=target, concurrency=1, force=False)
    return {
        "status": "accepted",
        "run_id": out["run_id"],
        "target_date": target.isoformat(),
        "type": NOAA_TYPE,
    }
