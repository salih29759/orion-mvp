from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import json
import logging
from pathlib import Path
import tempfile
from typing import Any

try:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
except Exception:  # noqa: BLE001
    boto3 = None  # type: ignore[assignment]
    UNSIGNED = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]
from google.cloud import storage
import pandas as pd
from sqlalchemy import select
import xarray as xr

from app.config import settings
from app.database import SessionLocal
from app.orm import AwsEra5ObjectORM, Era5ArtifactORM, Era5IngestJobORM, PortfolioAssetORM
from app.seed_data import RAW_PROVINCES

LOG = logging.getLogger("orion.aws_era5.ingestion")


@dataclass
class Point:
    point_id: str
    lat: float
    lon: float


@dataclass
class CachedObject:
    key: str
    variable: str
    gcs_uri: str
    local_path: Path
    etag: str | None
    byte_size: int
    cache_hit: bool


def _s3_client():
    if boto3 is None:
        raise RuntimeError("boto3 is required for AWS ERA5 ingestion operations")
    cfg = Config(signature_version=UNSIGNED) if settings.aws_era5_use_unsigned else None
    return boto3.client("s3", region_name=settings.aws_era5_region, config=cfg)


def _storage_client() -> storage.Client:
    return storage.Client()


def _file_sha256(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _collect_points(points_set: str | None) -> list[Point]:
    selected = points_set or settings.aws_era5_points_set_default
    include_assets = "assets" in selected
    include_provinces = "provinces" in selected

    out: dict[str, Point] = {}
    if include_assets:
        with SessionLocal() as db:
            rows = db.execute(select(PortfolioAssetORM.asset_id, PortfolioAssetORM.lat, PortfolioAssetORM.lon).distinct()).all()
        for asset_id, lat, lon in rows:
            pid = f"asset:{asset_id}"
            out[pid] = Point(point_id=pid, lat=float(lat), lon=float(lon))

    if include_provinces:
        for plate, _name, _region, lat, lon, *_ in RAW_PROVINCES:
            pid = f"province:{plate:02d}"
            out[pid] = Point(point_id=pid, lat=float(lat), lon=float(lon))

    return list(out.values())


def _normalize_coord_names(ds: xr.Dataset) -> xr.Dataset:
    rename_map: dict[str, str] = {}
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        rename_map["valid_time"] = "time"
    if "latitude" not in ds.coords and "lat" in ds.coords:
        rename_map["lat"] = "latitude"
    if "longitude" not in ds.coords and "lon" in ds.coords:
        rename_map["lon"] = "longitude"
    if rename_map:
        ds = ds.rename(rename_map)
    return ds


def _pick_data_var(ds: xr.Dataset) -> str:
    if not ds.data_vars:
        raise RuntimeError("dataset has no data_vars")
    if len(ds.data_vars) == 1:
        return next(iter(ds.data_vars.keys()))
    for candidate in ["2m_temperature", "total_precipitation", "10m_u_component_of_wind", "10m_v_component_of_wind", "volumetric_soil_water_layer_1"]:
        if candidate in ds.data_vars:
            return candidate
    return next(iter(ds.data_vars.keys()))


def _map_precip_components(df: pd.DataFrame) -> pd.DataFrame:
    cols = set(df.columns)
    if "total_precipitation" not in cols and {"large_scale_precipitation", "convective_precipitation"}.issubset(cols):
        df["total_precipitation"] = pd.to_numeric(df["large_scale_precipitation"], errors="coerce") + pd.to_numeric(
            df["convective_precipitation"], errors="coerce"
        )
    return df


def _to_daily_features(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly.empty:
        return hourly

    out = hourly.copy()
    out["time"] = pd.to_datetime(out["time"], utc=True, errors="coerce").dt.tz_convert(None)
    out = out.dropna(subset=["time"]) 
    out["date"] = out["time"].dt.date

    # Unit normalization before aggregation.
    if "2m_temperature" in out:
        t = pd.to_numeric(out["2m_temperature"], errors="coerce")
        if not t.dropna().empty and float(t.quantile(0.5)) > 150.0:
            out["2m_temperature"] = t - 273.15
        else:
            out["2m_temperature"] = t

    if "total_precipitation" in out:
        p = pd.to_numeric(out["total_precipitation"], errors="coerce")
        if not p.dropna().empty and float(p.quantile(0.99)) <= 5.0:
            out["total_precipitation"] = p * 1000.0
        else:
            out["total_precipitation"] = p

    if "10m_u_component_of_wind" in out and "10m_v_component_of_wind" in out:
        u = pd.to_numeric(out["10m_u_component_of_wind"], errors="coerce")
        v = pd.to_numeric(out["10m_v_component_of_wind"], errors="coerce")
        out["wind_speed"] = (u**2 + v**2) ** 0.5

    agg = (
        out.groupby(["point_id", "lat", "lng", "date"], dropna=False)
        .agg(
            temp_mean=("2m_temperature", "mean"),
            temp_max=("2m_temperature", "max"),
            precip_sum=("total_precipitation", "sum"),
            wind_max=("wind_speed", "max"),
            soil_moisture_mean=("volumetric_soil_water_layer_1", "mean"),
        )
        .reset_index()
    )
    agg["time"] = pd.to_datetime(agg["date"])
    agg["year"] = pd.to_datetime(agg["date"]).dt.year
    agg["month"] = pd.to_datetime(agg["date"]).dt.month
    return agg[["time", "year", "month", "point_id", "lat", "lng", "temp_mean", "temp_max", "precip_sum", "wind_max", "soil_moisture_mean"]]


def _cache_object_to_gcs(*, key: str, variable: str, year: int, month: int) -> CachedObject:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    s3 = _s3_client()
    gcs = _storage_client()
    bucket = gcs.bucket(settings.era5_gcs_bucket)

    head = s3.head_object(Bucket=settings.aws_era5_bucket, Key=key)
    etag = str(head.get("ETag", "")).replace('"', "") or None
    size = int(head.get("ContentLength", 0) or 0)

    object_name = f"raw/aws-era5/{variable}/{year:04d}/{month:02d}/{Path(key).name}"
    blob = bucket.blob(object_name)

    cache_hit = False
    if blob.exists():
        blob.reload()
        existing_etag = (blob.metadata or {}).get("source_etag") if blob.metadata else None
        if existing_etag and etag and existing_etag == etag:
            cache_hit = True

    local = Path(tempfile.gettempdir()) / f"aws_era5_{sha256(key.encode('utf-8')).hexdigest()[:16]}_{Path(key).name}"
    if cache_hit:
        blob.download_to_filename(str(local))
        return CachedObject(
            key=key,
            variable=variable,
            gcs_uri=f"gs://{settings.era5_gcs_bucket}/{object_name}",
            local_path=local,
            etag=etag,
            byte_size=size,
            cache_hit=True,
        )

    s3.download_file(settings.aws_era5_bucket, key, str(local))
    blob.metadata = {"source_etag": etag or ""}
    blob.upload_from_filename(str(local))

    return CachedObject(
        key=key,
        variable=variable,
        gcs_uri=f"gs://{settings.era5_gcs_bucket}/{object_name}",
        local_path=local,
        etag=etag,
        byte_size=size,
        cache_hit=False,
    )


def _extract_hourly_for_object(obj: CachedObject, points: list[Point]) -> pd.DataFrame:
    ds = xr.open_dataset(obj.local_path, engine="netcdf4")
    ds = _normalize_coord_names(ds)
    var_name = _pick_data_var(ds)
    da = ds[var_name]

    if "time" not in da.coords:
        raise RuntimeError(f"Missing time coordinate in {obj.key}")
    if "latitude" not in da.coords or "longitude" not in da.coords:
        raise RuntimeError(f"Missing latitude/longitude coordinates in {obj.key}")

    rows: list[dict[str, Any]] = []
    for point in points:
        sampled = da.sel(latitude=point.lat, longitude=point.lon, method="nearest").squeeze(drop=True)
        times = pd.to_datetime(sampled["time"].values, utc=True)
        vals = sampled.values
        if vals.ndim != 1:
            vals = vals.reshape(-1)
        grid_lat = float(sampled["latitude"].values)
        grid_lng = float(sampled["longitude"].values)
        for ts, value in zip(times, vals, strict=False):
            rows.append(
                {
                    "time": ts,
                    "point_id": point.point_id,
                    "lat": point.lat,
                    "lng": point.lon,
                    "grid_lat": grid_lat,
                    "grid_lng": grid_lng,
                    "variable": obj.variable,
                    "value": float(value) if pd.notna(value) else None,
                }
            )

    ds.close()
    return pd.DataFrame(rows)


def _select_month_objects(year: int, month: int, variables: list[str]) -> list[tuple[str, str]]:
    var_candidates: set[str] = set()
    for var in variables:
        if var == "total_precipitation":
            var_candidates.update(["total_precipitation", "large_scale_precipitation", "convective_precipitation"])
        else:
            var_candidates.add(var)

    with SessionLocal() as db:
        rows = db.execute(
            select(AwsEra5ObjectORM.key, AwsEra5ObjectORM.variable)
            .where(
                AwsEra5ObjectORM.year == year,
                AwsEra5ObjectORM.month == month,
                AwsEra5ObjectORM.variable.in_(sorted(var_candidates)),
            )
            .order_by(AwsEra5ObjectORM.key)
        ).all()
    return [(r[0], r[1]) for r in rows if r[0] and r[1]]


def _write_daily_partitions(job_id: str, daily: pd.DataFrame) -> int:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    if daily.empty:
        return 0

    client = _storage_client()
    bucket = client.bucket(settings.era5_gcs_bucket)
    files = 0
    daily = daily.copy()
    daily["date"] = pd.to_datetime(daily["time"]).dt.date
    for day, chunk in daily.groupby("date"):
        y = day.year
        m = day.month
        d = day.day
        local = Path(tempfile.gettempdir()) / f"orion_aws_daily_{job_id}_{day.isoformat()}.parquet"
        chunk.to_parquet(local, index=False)
        object_name = f"features/daily/year={y:04d}/month={m:02d}/day={d:02d}/part-{job_id}.parquet"
        bucket.blob(object_name).upload_from_filename(str(local))
        files += 1
    return files


def process_aws_era5_job(job_id: str) -> None:
    with SessionLocal() as db:
        job = db.get(Era5IngestJobORM, job_id)
        if not job:
            return
        req_vars = [v.strip() for v in job.variables_csv.split(",") if v.strip()]
        year = int(job.start_date.year)
        month = int(job.start_date.month)
        points = _collect_points(job.points_set)

    if not points:
        with SessionLocal() as db:
            job = db.get(Era5IngestJobORM, job_id)
            if job:
                job.status = "failed"
                job.error = "No points available for points extraction mode"
                job.finished_at = datetime.now(timezone.utc)
                db.commit()
        return

    objects = _select_month_objects(year, month, req_vars)
    if not objects:
        with SessionLocal() as db:
            job = db.get(Era5IngestJobORM, job_id)
            if job:
                job.status = "failed"
                job.error = f"No AWS catalog objects found for {year}-{month:02d}"
                job.finished_at = datetime.now(timezone.utc)
                db.commit()
        return

    cached: list[CachedObject] = []
    with ThreadPoolExecutor(max_workers=max(1, min(settings.aws_era5_max_concurrent_downloads, 3))) as ex:
        futs = [
            ex.submit(_cache_object_to_gcs, key=key, variable=var, year=year, month=month)
            for key, var in objects
        ]
        for fut in as_completed(futs):
            cached.append(fut.result())

    hourly_frames: list[pd.DataFrame] = []
    for obj in cached:
        frame = _extract_hourly_for_object(obj, points)
        if not frame.empty:
            hourly_frames.append(frame)

    if not hourly_frames:
        with SessionLocal() as db:
            job = db.get(Era5IngestJobORM, job_id)
            if job:
                job.status = "failed"
                job.error = "AWS extraction produced no hourly rows"
                job.finished_at = datetime.now(timezone.utc)
                db.commit()
        return

    hourly = pd.concat(hourly_frames, ignore_index=True)
    wide = hourly.pivot_table(index=["time", "point_id", "lat", "lng"], columns="variable", values="value", aggfunc="first").reset_index()
    wide.columns.name = None
    wide = _map_precip_components(wide)
    daily = _to_daily_features(wide)

    if daily.empty:
        with SessionLocal() as db:
            job = db.get(Era5IngestJobORM, job_id)
            if job:
                job.status = "failed"
                job.error = "AWS daily aggregation produced no rows"
                job.finished_at = datetime.now(timezone.utc)
                db.commit()
        return

    monthly_local = Path(tempfile.gettempdir()) / f"orion_aws_monthly_{job_id}_{year}_{month:02d}.parquet"
    daily.to_parquet(monthly_local, index=False)

    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    monthly_obj = f"features/era5_land/daily/year={year:04d}/month={month:02d}/{job_id}.parquet"
    client = _storage_client()
    bucket = client.bucket(settings.era5_gcs_bucket)
    bucket.blob(monthly_obj).upload_from_filename(str(monthly_local))
    monthly_uri = f"gs://{settings.era5_gcs_bucket}/{monthly_obj}"
    monthly_checksum = _file_sha256(monthly_local)

    partition_files = _write_daily_partitions(job_id, daily)

    dq_status = "pass"
    warnings: list[str] = []
    if (daily["precip_sum"].dropna() < 0).any() or (daily["wind_max"].dropna() < 0).any():
        dq_status = "fail"
    if daily["soil_moisture_mean"].dropna().empty:
        warnings.append("soil_moisture_missing")
        if dq_status == "pass":
            dq_status = "warn"

    raw_bytes = sum(o.byte_size for o in cached)
    feature_bytes = int(monthly_local.stat().st_size)
    feature_rows = int(len(daily))

    with SessionLocal() as db:
        job = db.get(Era5IngestJobORM, job_id)
        if not job:
            return

        for obj in cached:
            db.add(
                Era5ArtifactORM(
                    request_signature=job.request_signature,
                    job_id=job.job_id,
                    artifact_type="raw_nc",
                    dataset=job.dataset,
                    variables_csv=job.variables_csv,
                    bbox_csv=job.bbox_csv,
                    start_date=job.start_date,
                    end_date=job.end_date,
                    gcs_uri=obj.gcs_uri,
                    source_uri=f"s3://{settings.aws_era5_bucket}/{obj.key}",
                    source_etag=obj.etag,
                    cache_hit=obj.cache_hit,
                    checksum_sha256=_file_sha256(obj.local_path),
                    byte_size=obj.byte_size,
                )
            )

        db.add(
            Era5ArtifactORM(
                request_signature=job.request_signature,
                job_id=job.job_id,
                artifact_type="feature_daily_parquet",
                dataset=job.dataset,
                variables_csv=job.variables_csv,
                bbox_csv=job.bbox_csv,
                start_date=job.start_date,
                end_date=job.end_date,
                gcs_uri=monthly_uri,
                source_uri=None,
                source_etag=None,
                cache_hit=False,
                checksum_sha256=monthly_checksum,
                byte_size=feature_bytes,
            )
        )

        job.rows_written = feature_rows
        job.bytes_downloaded = raw_bytes + feature_bytes
        job.raw_files = len(cached)
        job.feature_files = 1 + partition_files
        job.dq_status = "fail" if dq_status == "fail" else ("warn" if dq_status == "warn" else "pass")
        job.dq_report_json = json.dumps(
            {
                "provider": "aws_nsf_ncar",
                "point_mode": "points",
                "points_count": len(points),
                "objects_selected": len(objects),
                "raw_cache_hits": sum(1 for o in cached if o.cache_hit),
                "warnings": warnings,
            }
        )
        job.duration_seconds = (datetime.now(timezone.utc) - (job.started_at or datetime.now(timezone.utc))).total_seconds()
        job.finished_at = datetime.now(timezone.utc)
        if dq_status == "fail":
            job.status = "fail_dq"
        elif dq_status == "warn":
            job.status = "success_with_warnings"
        else:
            job.status = "success"
        db.commit()

    LOG.info(
        json.dumps(
            {
                "event": "aws_era5_job_finish",
                "job_id": job_id,
                "month": f"{year:04d}-{month:02d}",
                "rows_written": feature_rows,
                "raw_files": len(cached),
                "partition_files": partition_files,
                "points": len(points),
            }
        )
    )
