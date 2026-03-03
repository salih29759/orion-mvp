from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import json
import logging
from pathlib import Path
import re
import tempfile
import threading
import time
from uuid import uuid4

import cdsapi
from google.cloud import storage
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
import xarray as xr
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import GlofasBackfillItemORM, GlofasBackfillJobORM, ProvinceORM

LOG = logging.getLogger("orion.glofas")

GLOFAS_DATASET = "cems-glofas-historical"
GLOFAS_VARIABLE = "river_discharge_in_the_last_24_hours"
GLOFAS_AREA = [42, 26, 36, 45]
BASELINE_OBJECT_NAME = "climatology/glofas/baseline.parquet"
FEATURE_OBJECT_FMT = "features/daily/glofas/year={year:04d}/month={month:02d}/part-0.parquet"
FEATURE_MONTH_RE = re.compile(r"features/daily/glofas/year=(\d{4})/month=(\d{2})/.+\\.parquet$")


@dataclass
class ProvincePoint:
    point_id: str
    lat: float
    lon: float


def _storage_client() -> storage.Client:
    return storage.Client()


def validate_glofas_runtime() -> list[str]:
    missing: list[str] = []
    if not settings.era5_gcs_bucket:
        missing.append("ERA5_GCS_BUCKET")
    if not (settings.glofas_api_key or settings.cdsapi_key):
        missing.append("GLOFAS_API_KEY/CDSAPI_KEY")
    return missing


def _month_specs(start: date, end: date) -> list[tuple[str, int, int]]:
    cur = date(start.year, start.month, 1)
    out: list[tuple[str, int, int]] = []
    while cur <= end:
        out.append((f"{cur.year:04d}-{cur.month:02d}", cur.year, cur.month))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def _request_signature(*, start: date, end: date, effective_end: date, concurrency: int) -> str:
    payload = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "effective_end": effective_end.isoformat(),
        "concurrency": concurrency,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _load_province_points() -> list[ProvincePoint]:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM).order_by(ProvinceORM.plate)).scalars().all()
    return [
        ProvincePoint(point_id=f"province:{int(r.plate):02d}", lat=float(r.lat), lon=float(r.lng))
        for r in rows
    ]


def _retrieve_month_grib(*, year: int, month: int, target: Path) -> None:
    key = settings.glofas_api_key or settings.cdsapi_key
    if not key:
        raise RuntimeError("GLOFAS auth key is missing")

    req = {
        "system_version": "version_4_0",
        "hydrological_model": "lisflood",
        "product_type": "consolidated",
        "variable": GLOFAS_VARIABLE,
        "hyear": f"{year:04d}",
        "hmonth": f"{month:02d}",
        "hday": [f"{d:02d}" for d in range(1, 32)],
        "data_format": "grib",
        "area": GLOFAS_AREA,
    }

    client = cdsapi.Client(url=settings.glofas_api_url, key=key, quiet=True)
    last_error: str | None = None
    for attempt in range(1, 6):
        try:
            t0 = time.time()
            client.retrieve(GLOFAS_DATASET, req, str(target))
            LOG.info(
                "glofas_retrieve_success year=%s month=%s attempt=%s seconds=%.2f",
                year,
                month,
                attempt,
                time.time() - t0,
            )
            return
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            wait = min(60, 2 ** attempt)
            LOG.warning(
                "glofas_retrieve_retry year=%s month=%s attempt=%s wait=%ss error=%s",
                year,
                month,
                attempt,
                wait,
                last_error,
            )
            time.sleep(wait)
    raise RuntimeError(f"GloFAS retrieve failed for {year:04d}-{month:02d}: {last_error}")


def _open_glofas_dataset(grib_path: Path) -> xr.Dataset:
    try:
        import cfgrib  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("cfgrib is required for GloFAS GRIB processing") from exc

    ds = xr.open_dataset(
        grib_path,
        engine="cfgrib",
        backend_kwargs={"indexpath": ""},
    )
    rename: dict[str, str] = {}
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        rename["valid_time"] = "time"
    if "lat" in ds.coords and "latitude" not in ds.coords:
        rename["lat"] = "latitude"
    if "lon" in ds.coords and "longitude" not in ds.coords:
        rename["lon"] = "longitude"
    if rename:
        ds = ds.rename(rename)
    return ds


def _resolve_discharge_var(ds: xr.Dataset) -> str:
    if GLOFAS_VARIABLE in ds.data_vars:
        return GLOFAS_VARIABLE
    if "dis24" in ds.data_vars:
        return "dis24"
    if len(ds.data_vars) == 1:
        return next(iter(ds.data_vars.keys()))
    for name in ds.data_vars:
        if "discharge" in name.lower() or "dis24" in name.lower():
            return name
    raise RuntimeError(f"Could not resolve discharge variable. vars={sorted(ds.data_vars.keys())}")


def _river_mask_coords(ds: xr.Dataset, var_name: str) -> tuple[np.ndarray, np.ndarray]:
    arr = np.asarray(ds[var_name].values)
    if arr.ndim == 3:
        mask = np.any(np.isfinite(arr) & (arr > 0), axis=0)
    elif arr.ndim == 2:
        mask = np.isfinite(arr) & (arr > 0)
    else:
        raise RuntimeError(f"Unexpected discharge array shape: {arr.shape}")

    lat = np.asarray(ds["latitude"].values)
    lon = np.asarray(ds["longitude"].values)
    if lat.ndim == 1 and lon.ndim == 1:
        lat_grid, lon_grid = np.meshgrid(lat, lon, indexing="ij")
    elif lat.ndim == 2 and lon.ndim == 2:
        lat_grid, lon_grid = lat, lon
    else:
        raise RuntimeError("Unsupported latitude/longitude dimensions")

    cell_lats = lat_grid[mask].astype(float)
    cell_lons = lon_grid[mask].astype(float)
    if cell_lats.size == 0:
        raise RuntimeError("No valid river cells found in this month")
    return cell_lats, cell_lons


def _build_point_mapping(ds: xr.Dataset, var_name: str, points: list[ProvincePoint]) -> dict[str, tuple[float, float]]:
    cell_lats, cell_lons = _river_mask_coords(ds, var_name)
    tree = cKDTree(np.column_stack([cell_lats, cell_lons]))
    mapping: dict[str, tuple[float, float]] = {}
    for p in points:
        _dist, idx = tree.query([p.lat, p.lon], k=1)
        mapping[p.point_id] = (float(cell_lats[idx]), float(cell_lons[idx]))
    return mapping


def _calculate_baseline_frame(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce")
    base = base.dropna(subset=["date", "point_id", "river_discharge_m3s"])
    base["doy"] = base["date"].dt.dayofyear
    grouped = base.groupby(["point_id", "doy"], as_index=False)["river_discharge_m3s"]
    out = grouped.agg(mean="mean", p50=lambda s: s.quantile(0.5), p90=lambda s: s.quantile(0.9))
    out["doy"] = out["doy"].astype(int)
    return out


def _build_baseline_lookup(baseline_df: pd.DataFrame) -> dict[tuple[str, int], tuple[float | None, float | None]]:
    lookup: dict[tuple[str, int], tuple[float | None, float | None]] = {}
    for _, row in baseline_df.iterrows():
        key = (str(row["point_id"]), int(row["doy"]))
        mean = None if pd.isna(row["mean"]) else float(row["mean"])
        p90 = None if pd.isna(row["p90"]) else float(row["p90"])
        lookup[key] = (mean, p90)
    return lookup


def _enrich_with_baseline(
    df: pd.DataFrame,
    baseline_lookup: dict[tuple[str, int], tuple[float | None, float | None]],
) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["doy"] = out["date"].dt.dayofyear.astype("Int64")

    anomalies: list[float | None] = []
    flags: list[bool] = []
    for _, row in out.iterrows():
        point_id = str(row["point_id"])
        doy = int(row["doy"])
        val = None if pd.isna(row["river_discharge_m3s"]) else float(row["river_discharge_m3s"])
        mean, p90 = baseline_lookup.get((point_id, doy), (None, None))
        if val is None or mean is None or mean <= 0:
            anomalies.append(None)
        else:
            anomalies.append(((val - mean) / mean) * 100.0)
        flags.append(bool(val is not None and p90 is not None and val > p90))

    out["discharge_anomaly_pct"] = anomalies
    out["flood_flag"] = flags
    out["date"] = out["date"].dt.date
    return out.drop(columns=["doy"])


def _build_month_frame(
    ds: xr.Dataset,
    *,
    var_name: str,
    points: list[ProvincePoint],
    point_to_grid: dict[str, tuple[float, float]],
    run_id: str,
    baseline_lookup: dict[tuple[str, int], tuple[float | None, float | None]],
    ingested_at: datetime,
) -> pd.DataFrame:
    times = pd.to_datetime(ds["time"].values, errors="coerce")
    rows: list[dict[str, object]] = []
    for point in points:
        grid_lat, grid_lon = point_to_grid[point.point_id]
        series = ds[var_name].sel(latitude=grid_lat, longitude=grid_lon, method="nearest").values
        vals = np.asarray(series).reshape(-1)
        for ts, value in zip(times, vals):
            if pd.isna(ts):
                continue
            rows.append(
                {
                    "date": ts,
                    "point_id": point.point_id,
                    "lat": float(point.lat),
                    "lon": float(point.lon),
                    "river_discharge_m3s": None if pd.isna(value) else float(value),
                }
            )

    if not rows:
        raise RuntimeError("No discharge rows extracted for month")

    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame = (
        frame.groupby(["date", "point_id", "lat", "lon"], as_index=False)["river_discharge_m3s"]
        .mean()
        .sort_values(["date", "point_id"])
    )

    if baseline_lookup:
        frame = _enrich_with_baseline(frame, baseline_lookup)
    else:
        frame["discharge_anomaly_pct"] = None
        frame["flood_flag"] = False

    frame["source"] = "glofas_v4"
    frame["run_id"] = run_id
    frame["ingested_at"] = ingested_at
    return frame[
        [
            "date",
            "point_id",
            "lat",
            "lon",
            "river_discharge_m3s",
            "discharge_anomaly_pct",
            "flood_flag",
            "source",
            "run_id",
            "ingested_at",
        ]
    ]


def _upload_parquet_to_gcs(local_path: Path, object_name: str) -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    client = _storage_client()
    blob = client.bucket(settings.era5_gcs_bucket).blob(object_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{settings.era5_gcs_bucket}/{object_name}"


def _baseline_uri(bucket: str) -> str:
    return f"gs://{bucket}/{BASELINE_OBJECT_NAME}"


def _download_blob_to_temp(bucket_name: str, object_name: str, suffix: str) -> Path:
    target = Path(tempfile.gettempdir()) / f"orion_glofas_{uuid4().hex}{suffix}"
    client = _storage_client()
    client.bucket(bucket_name).blob(object_name).download_to_filename(str(target))
    return target


def _load_baseline_from_gcs() -> tuple[dict[tuple[str, int], tuple[float | None, float | None]], str | None]:
    if not settings.era5_gcs_bucket:
        return {}, None
    client = _storage_client()
    bucket = client.bucket(settings.era5_gcs_bucket)
    blob = bucket.blob(BASELINE_OBJECT_NAME)
    if not blob.exists():
        return {}, None

    local = Path(tempfile.gettempdir()) / f"orion_glofas_baseline_{uuid4().hex}.parquet"
    blob.download_to_filename(str(local))
    df = pd.read_parquet(local)
    required = {"point_id", "doy", "mean", "p50", "p90"}
    if not required.issubset(set(df.columns)):
        raise RuntimeError("Existing GloFAS baseline schema is invalid")
    return _build_baseline_lookup(df), _baseline_uri(settings.era5_gcs_bucket)


def _required_baseline_months() -> set[str]:
    out: set[str] = set()
    cur = date(1979, 1, 1)
    end = date(2010, 12, 1)
    while cur <= end:
        out.add(f"{cur.year:04d}-{cur.month:02d}")
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


def _list_glofas_feature_blobs() -> list[tuple[str, str]]:
    if not settings.era5_gcs_bucket:
        return []
    client = _storage_client()
    blobs = client.list_blobs(settings.era5_gcs_bucket, prefix="features/daily/glofas/")
    out: list[tuple[str, str]] = []
    for blob in blobs:
        match = FEATURE_MONTH_RE.match(blob.name)
        if not match:
            continue
        month_label = f"{int(match.group(1)):04d}-{int(match.group(2)):02d}"
        out.append((month_label, blob.name))
    return out


def _try_build_baseline_from_features() -> str | None:
    if not settings.era5_gcs_bucket:
        return None

    month_blobs = _list_glofas_feature_blobs()
    if not month_blobs:
        return None

    required = _required_baseline_months()
    by_month: dict[str, list[str]] = {}
    for month_label, object_name in month_blobs:
        by_month.setdefault(month_label, []).append(object_name)

    if not required.issubset(set(by_month.keys())):
        return None

    frames: list[pd.DataFrame] = []
    for month_label in sorted(required):
        object_names = by_month.get(month_label, [])
        for object_name in object_names:
            local = _download_blob_to_temp(settings.era5_gcs_bucket, object_name, ".parquet")
            df = pd.read_parquet(local, columns=["date", "point_id", "river_discharge_m3s"])
            if df.empty:
                continue
            frames.append(df)

    if not frames:
        return None

    baseline_df = _calculate_baseline_frame(pd.concat(frames, ignore_index=True))
    if baseline_df.empty:
        return None

    local_out = Path(tempfile.gettempdir()) / f"orion_glofas_baseline_{uuid4().hex}.parquet"
    baseline_df.to_parquet(local_out, index=False)
    return _upload_parquet_to_gcs(local_out, BASELINE_OBJECT_NAME)


def _load_or_build_baseline() -> tuple[dict[tuple[str, int], tuple[float | None, float | None]], str | None]:
    lookup, uri = _load_baseline_from_gcs()
    if uri:
        return lookup, uri
    built_uri = _try_build_baseline_from_features()
    if not built_uri:
        return {}, None
    return _load_baseline_from_gcs()


def _mark_item_running(job_id: str, month_label: str) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        item = db.execute(
            select(GlofasBackfillItemORM)
            .where(
                GlofasBackfillItemORM.job_id == job_id,
                GlofasBackfillItemORM.month_label == month_label,
            )
            .limit(1)
        ).scalar_one_or_none()
        if not item:
            return
        item.status = "running"
        item.started_at = now
        db.commit()


def _mark_item_success(job_id: str, month_label: str, *, rows_written: int, output_gcs_uri: str) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        item = db.execute(
            select(GlofasBackfillItemORM)
            .where(
                GlofasBackfillItemORM.job_id == job_id,
                GlofasBackfillItemORM.month_label == month_label,
            )
            .limit(1)
        ).scalar_one_or_none()
        if not item:
            return
        item.status = "success"
        item.rows_written = rows_written
        item.output_gcs_uri = output_gcs_uri
        item.error = None
        item.finished_at = now
        db.commit()


def _mark_item_failed(job_id: str, month_label: str, *, error: str) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        item = db.execute(
            select(GlofasBackfillItemORM)
            .where(
                GlofasBackfillItemORM.job_id == job_id,
                GlofasBackfillItemORM.month_label == month_label,
            )
            .limit(1)
        ).scalar_one_or_none()
        if not item:
            return
        item.status = "failed"
        item.error = error[:2000]
        item.finished_at = now
        db.commit()


def _refresh_job_progress(job_id: str) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        job = db.get(GlofasBackfillJobORM, job_id)
        if not job:
            return
        items = db.execute(
            select(GlofasBackfillItemORM).where(GlofasBackfillItemORM.job_id == job_id)
        ).scalars().all()
        success = sum(1 for item in items if item.status == "success")
        failed = sum(1 for item in items if item.status == "failed")
        job.months_success = success
        job.months_failed = failed
        job.updated_at = now
        all_done = all(item.status in {"success", "failed"} for item in items)
        if all_done:
            job.status = "failed" if failed else "success"
            job.finished_at = now
        db.commit()


def _process_month(
    *,
    job_id: str,
    month_label: str,
    points: list[ProvincePoint],
    baseline_lookup: dict[tuple[str, int], tuple[float | None, float | None]],
    mapping_ref: dict[str, dict[str, tuple[float, float]]],
    mapping_lock: threading.Lock,
) -> tuple[int, str]:
    year, month = [int(part) for part in month_label.split("-")]
    grib_path = Path(tempfile.gettempdir()) / f"orion_glofas_{job_id}_{year:04d}_{month:02d}.grib"

    _retrieve_month_grib(year=year, month=month, target=grib_path)
    ds = _open_glofas_dataset(grib_path)
    var_name = _resolve_discharge_var(ds)

    if "mapping" not in mapping_ref:
        with mapping_lock:
            if "mapping" not in mapping_ref:
                mapping_ref["mapping"] = _build_point_mapping(ds, var_name, points)

    ingested_at = datetime.now(timezone.utc)
    frame = _build_month_frame(
        ds,
        var_name=var_name,
        points=points,
        point_to_grid=mapping_ref["mapping"],
        run_id=job_id,
        baseline_lookup=baseline_lookup,
        ingested_at=ingested_at,
    )
    ds.close()

    local_out = Path(tempfile.gettempdir()) / f"orion_glofas_features_{job_id}_{year:04d}_{month:02d}.parquet"
    frame.to_parquet(local_out, index=False)
    object_name = FEATURE_OBJECT_FMT.format(year=year, month=month)
    uri = _upload_parquet_to_gcs(local_out, object_name)
    return int(len(frame)), uri


def start_glofas_background_job(job_id: str) -> None:
    t = threading.Thread(target=process_glofas_backfill, args=(job_id,), daemon=False)
    t.start()


def submit_glofas_backfill(*, start: date, end: date, concurrency: int) -> tuple[str, bool, int]:
    effective_end = min(end, datetime.now(timezone.utc).date())
    if start > effective_end:
        raise RuntimeError("start must be <= effective_end")

    req_sig = _request_signature(start=start, end=end, effective_end=effective_end, concurrency=concurrency)
    month_specs = _month_specs(start, effective_end)

    with SessionLocal() as db:
        existing = db.execute(
            select(GlofasBackfillJobORM)
            .where(
                GlofasBackfillJobORM.request_signature == req_sig,
                GlofasBackfillJobORM.status.in_(["queued", "running", "success"]),
            )
            .order_by(desc(GlofasBackfillJobORM.created_at))
            .limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing.job_id, True, existing.months_total

        job_id = str(uuid4())
        now = datetime.now(timezone.utc)
        db.add(
            GlofasBackfillJobORM(
                job_id=job_id,
                request_signature=req_sig,
                status="queued",
                start_date=start,
                end_date=end,
                effective_end_date=effective_end,
                concurrency=max(1, min(int(concurrency), 4)),
                months_total=len(month_specs),
                months_success=0,
                months_failed=0,
                baseline_ready=False,
                baseline_gcs_uri=None,
                error=None,
                created_at=now,
                started_at=None,
                updated_at=now,
                finished_at=None,
            )
        )
        for month_label, _year, _month in month_specs:
            db.add(
                GlofasBackfillItemORM(
                    job_id=job_id,
                    month_label=month_label,
                    status="queued",
                    rows_written=0,
                    output_gcs_uri=None,
                    error=None,
                )
            )
        db.commit()

    start_glofas_background_job(job_id)
    return job_id, False, len(month_specs)


def process_glofas_backfill(job_id: str) -> None:
    with SessionLocal() as db:
        job = db.get(GlofasBackfillJobORM, job_id)
        if not job:
            return
        job.status = "running"
        job.started_at = job.started_at or datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)
        job.error = None
        db.commit()

    points = _load_province_points()
    if not points:
        with SessionLocal() as db:
            job = db.get(GlofasBackfillJobORM, job_id)
            if job:
                job.status = "failed"
                job.error = "No provinces found"
                job.finished_at = datetime.now(timezone.utc)
                job.updated_at = datetime.now(timezone.utc)
                db.commit()
        return

    baseline_lookup: dict[tuple[str, int], tuple[float | None, float | None]] = {}
    baseline_uri: str | None = None
    try:
        baseline_lookup, baseline_uri = _load_or_build_baseline()
    except Exception as exc:  # noqa: BLE001
        LOG.warning("glofas_baseline_load_failed job_id=%s error=%s", job_id, str(exc))

    with SessionLocal() as db:
        job = db.get(GlofasBackfillJobORM, job_id)
        if job:
            job.baseline_ready = bool(baseline_uri)
            job.baseline_gcs_uri = baseline_uri
            job.updated_at = datetime.now(timezone.utc)
            db.commit()

    with SessionLocal() as db:
        items = db.execute(
            select(GlofasBackfillItemORM)
            .where(GlofasBackfillItemORM.job_id == job_id)
            .order_by(GlofasBackfillItemORM.month_label)
        ).scalars().all()
        month_labels = [item.month_label for item in items]

    mapping_ref: dict[str, dict[str, tuple[float, float]]] = {}
    mapping_lock = threading.Lock()
    concurrency = 1
    with SessionLocal() as db:
        job = db.get(GlofasBackfillJobORM, job_id)
        if job:
            concurrency = max(1, min(int(job.concurrency or 1), 4))

    def _run_month(month_label: str) -> tuple[str, int, str]:
        _mark_item_running(job_id, month_label)
        rows, uri = _process_month(
            job_id=job_id,
            month_label=month_label,
            points=points,
            baseline_lookup=baseline_lookup,
            mapping_ref=mapping_ref,
            mapping_lock=mapping_lock,
        )
        return month_label, rows, uri

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = {ex.submit(_run_month, month_label): month_label for month_label in month_labels}
            for fut in as_completed(futures):
                month_label = futures[fut]
                try:
                    _ml, rows, uri = fut.result()
                    _mark_item_success(job_id, month_label, rows_written=rows, output_gcs_uri=uri)
                except Exception as exc:  # noqa: BLE001
                    _mark_item_failed(job_id, month_label, error=str(exc))
                    LOG.exception("glofas_month_failed job_id=%s month=%s error=%s", job_id, month_label, str(exc))
                finally:
                    _refresh_job_progress(job_id)
    except Exception as exc:  # noqa: BLE001
        with SessionLocal() as db:
            job = db.get(GlofasBackfillJobORM, job_id)
            if job:
                job.status = "failed"
                job.error = str(exc)
                job.finished_at = datetime.now(timezone.utc)
                job.updated_at = datetime.now(timezone.utc)
                db.commit()
        return

    if not baseline_uri:
        try:
            lookup, built_uri = _load_or_build_baseline()
            if built_uri:
                baseline_lookup = lookup
                with SessionLocal() as db:
                    job = db.get(GlofasBackfillJobORM, job_id)
                    if job:
                        job.baseline_ready = True
                        job.baseline_gcs_uri = built_uri
                        job.updated_at = datetime.now(timezone.utc)
                        db.commit()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("glofas_baseline_build_failed job_id=%s error=%s", job_id, str(exc))

    _refresh_job_progress(job_id)


def get_latest_glofas_status() -> dict | None:
    with SessionLocal() as db:
        job = db.execute(
            select(GlofasBackfillJobORM)
            .order_by(desc(GlofasBackfillJobORM.created_at))
            .limit(1)
        ).scalar_one_or_none()
        if not job:
            return None

        items = db.execute(
            select(GlofasBackfillItemORM)
            .where(GlofasBackfillItemORM.job_id == job.job_id)
            .order_by(GlofasBackfillItemORM.month_label)
        ).scalars().all()

        total = len(items)
        completed = sum(1 for item in items if item.status == "success")
        failed = sum(1 for item in items if item.status == "failed")
        running = sum(1 for item in items if item.status == "running")
        failed_months = [item.month_label for item in items if item.status == "failed"]
        percent = round(((completed + failed) / total) * 100.0, 2) if total else 100.0

        eta_hours = None
        started_at = job.started_at or job.created_at
        if started_at and completed > 0 and total > (completed + failed):
            elapsed_hours = max((datetime.now(timezone.utc) - started_at).total_seconds() / 3600.0, 1e-6)
            rate = completed / elapsed_hours
            remaining = max(total - completed - failed, 0)
            eta_hours = round(remaining / rate, 2) if rate > 0 else None

        if total and (completed + failed) == total and job.status in {"queued", "running"}:
            job.status = "failed" if failed else "success"
            job.finished_at = datetime.now(timezone.utc)
            job.updated_at = datetime.now(timezone.utc)
            db.commit()

        last_updated = job.updated_at or job.finished_at or job.started_at or job.created_at

        return {
            "run_id": job.job_id,
            "status": job.status,
            "total_months": total,
            "completed": completed,
            "failed": failed,
            "running": running,
            "percent_done": percent,
            "failed_months": failed_months,
            "eta_hours": eta_hours,
            "last_updated": last_updated.isoformat(),
            "baseline_ready": bool(job.baseline_ready),
            "baseline_uri": job.baseline_gcs_uri,
        }
