from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from calendar import monthrange
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
import json
import logging
import tempfile
import threading
import time
import zipfile
from uuid import uuid4

import cdsapi
from google.cloud import storage
import pandas as pd
from sqlalchemy import desc, select
from sqlalchemy.orm import Session
import xarray as xr

from app.config import settings
from app.database import SessionLocal
from app.orm import (
    Era5ArtifactORM,
    Era5BackfillItemORM,
    Era5BackfillJobORM,
    Era5IngestJobORM,
    ExportJobORM,
)

LOG = logging.getLogger("orion.era5")


@dataclass
class Era5Request:
    start_date: date
    end_date: date
    bbox: tuple[float, float, float, float]  # north, west, south, east
    variables: list[str]
    dataset: str
    out_format: str = "netcdf"
    provider: str = "cds"
    mode: str = "bbox"
    points_set: str | None = None
    month_label: str | None = None
    source_range_json: str | None = None


@dataclass
class DailyFeatureBuildResult:
    rows: int
    dq_report: dict


ERA5_VAR_ALIASES = {
    "t2m": "2m_temperature",
    "tp": "total_precipitation",
    "u10": "10m_u_component_of_wind",
    "v10": "10m_v_component_of_wind",
    "swvl1": "volumetric_soil_water_layer_1",
}


def validate_era5_runtime() -> list[str]:
    missing: list[str] = []
    if not settings.cdsapi_key:
        missing.append("CDSAPI_KEY")
    if not settings.era5_gcs_bucket:
        missing.append("ERA5_GCS_BUCKET")
    return missing


def request_signature(req: Era5Request) -> str:
    payload = {
        "dataset": req.dataset,
        "variables": sorted(req.variables),
        "bbox": [round(x, 4) for x in req.bbox],
        "start_date": req.start_date.isoformat(),
        "end_date": req.end_date.isoformat(),
        "format": req.out_format,
        "provider": req.provider,
        "mode": req.mode,
        "points_set": req.points_set,
        "month_label": req.month_label,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _month_chunks(start: date, end: date) -> list[tuple[int, int, list[str]]]:
    chunks: list[tuple[int, int, list[str]]] = []
    cur = date(start.year, start.month, 1)
    while cur <= end:
        month_start = max(start, cur)
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = min(end, next_month - timedelta(days=1))
        days = [f"{d:02d}" for d in range(month_start.day, month_end.day + 1)]
        chunks.append((cur.year, cur.month, days))
        cur = next_month
    return chunks


def _file_sha256(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _storage_client() -> storage.Client:
    return storage.Client()


def _upload_to_gcs(local_file: Path, object_name: str) -> tuple[str, int]:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    client = _storage_client()
    bucket = client.bucket(settings.era5_gcs_bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_file))
    size = local_file.stat().st_size
    return f"gs://{settings.era5_gcs_bucket}/{object_name}", size


def _download_gcs_uri(gcs_uri: str) -> Path:
    if not gcs_uri.startswith("gs://"):
        raise ValueError("Invalid gcs uri")
    _, rest = gcs_uri.split("gs://", 1)
    bucket_name, object_name = rest.split("/", 1)
    target = Path(tempfile.gettempdir()) / f"orion_{uuid4().hex}_{Path(object_name).name}"
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.download_to_filename(str(target))
    return target


@lru_cache(maxsize=1024)
def _cached_nearest_coord(gcs_uri: str, lat_rounded: float, lng_rounded: float) -> tuple[float, float] | None:
    local_path = _download_gcs_uri(gcs_uri)
    df = pd.read_parquet(local_path, columns=["lat", "lng"])
    if df.empty:
        return None
    coord = (
        df[["lat", "lng"]]
        .drop_duplicates()
        .assign(_dist=lambda d: (d["lat"] - lat_rounded).abs() + (d["lng"] - lng_rounded).abs())
        .sort_values("_dist")
        .head(1)
    )
    if coord.empty:
        return None
    return float(coord.iloc[0]["lat"]), float(coord.iloc[0]["lng"])


def _retrieve_month(req: Era5Request, year: int, month: int, days: list[str], target: Path) -> None:
    if not settings.cdsapi_key:
        raise RuntimeError("CDSAPI_KEY is missing")

    request = {
        "product_type": "reanalysis",
        "variable": req.variables,
        "year": [str(year)],
        "month": [f"{month:02d}"],
        "day": days,
        "time": [f"{h:02d}:00" for h in range(24)],
        "area": [req.bbox[0], req.bbox[1], req.bbox[2], req.bbox[3]],
    }

    client = cdsapi.Client(url=settings.cdsapi_url, key=settings.cdsapi_key, quiet=True)
    attempts = [
        {**request, "format": req.out_format},
        {**request, "data_format": req.out_format, "download_format": "unarchived"},
    ]
    last_error = None
    dataset = "reanalysis-era5-land" if req.dataset == "era5-land" else req.dataset

    for payload in attempts:
        for i in range(1, 6):
            try:
                t0 = time.time()
                client.retrieve(dataset, payload, str(target))
                LOG.info(
                    "era5_retrieve_success dataset=%s year=%s month=%s attempt=%s seconds=%.2f",
                    dataset,
                    year,
                    month,
                    i,
                    time.time() - t0,
                )
                return
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                lower_err = last_error.lower()
                if (
                    "401" in last_error
                    or "unauthorized" in lower_err
                    or "operation not allowed" in lower_err
                    or "authentication failed" in lower_err
                ):
                    raise RuntimeError(
                        "CDS authorization failed. Verify CDSAPI_KEY and accept terms for "
                        "reanalysis-era5-land dataset in CDS portal."
                    ) from exc
                wait = min(60, 2 ** i)
                LOG.warning(
                    "era5_retrieve_retry dataset=%s year=%s month=%s attempt=%s wait=%ss error=%s",
                    dataset,
                    year,
                    month,
                    i,
                    wait,
                    last_error,
                )
                time.sleep(wait)
    raise RuntimeError(f"CDS retrieve failed for {year}-{month:02d}: {last_error}")


def _build_daily_features(nc_path: Path, out_parquet: Path) -> DailyFeatureBuildResult:
    dataset_path = nc_path
    with nc_path.open("rb") as f:
        sig = f.read(4)
    if sig.startswith(b"PK\x03\x04"):
        out_dir = Path(tempfile.gettempdir()) / f"orion_era5_unzip_{uuid4().hex}"
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(nc_path, "r") as zf:
            zf.extractall(out_dir)
        nc_files = sorted(out_dir.rglob("*.nc"))
        if not nc_files:
            raise RuntimeError("CDS archive did not contain a netcdf file")
        dataset_path = nc_files[0]

    ds = xr.open_dataset(dataset_path, engine="netcdf4")
    rename_map: dict[str, str] = {}
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        rename_map["valid_time"] = "time"
    for old, new in ERA5_VAR_ALIASES.items():
        if old in ds.data_vars and new not in ds.data_vars:
            rename_map[old] = new
    if rename_map:
        ds = ds.rename(rename_map)

    vars_present = set(ds.data_vars)
    if "time" not in ds.coords and "time" not in ds.dims:
        raise RuntimeError(
            f"Dataset missing time axis after normalization. dims={dict(ds.dims)} vars={sorted(vars_present)}"
        )

    data = xr.Dataset()
    if "2m_temperature" in vars_present:
        t2m = ds["2m_temperature"]
        t_mean = t2m.resample(time="1D").mean()
        t_max = t2m.resample(time="1D").max()
        # Convert Kelvin to Celsius only when values indicate Kelvin.
        if float(t_mean.quantile(0.5).values) > 150:
            t_mean = t_mean - 273.15
            t_max = t_max - 273.15
        data["temp_mean"] = t_mean
        data["temp_max"] = t_max
    if "total_precipitation" in vars_present:
        tp = ds["total_precipitation"]
        p_sum = tp.resample(time="1D").sum()
        # ERA5 tp is typically meters; convert to mm for canonical storage.
        if float(p_sum.quantile(0.99).values) <= 5.0:
            p_sum = p_sum * 1000.0
        data["precip_sum"] = p_sum
    if "10m_u_component_of_wind" in vars_present and "10m_v_component_of_wind" in vars_present:
        wind = (ds["10m_u_component_of_wind"] ** 2 + ds["10m_v_component_of_wind"] ** 2) ** 0.5
        data["wind_max"] = wind.resample(time="1D").max()
    if "volumetric_soil_water_layer_1" in vars_present:
        data["soil_moisture_mean"] = ds["volumetric_soil_water_layer_1"].resample(time="1D").mean()

    if not data.data_vars:
        raise RuntimeError(
            "No supported ERA5 variables found in dataset. "
            f"available_vars={sorted(vars_present)} expected_any={sorted(ERA5_VAR_ALIASES.values())}"
        )

    # Keep only indexed coords; scalar coords (e.g., number/expver) can break DataFrame conversion.
    data = data.reset_coords(drop=True)
    df = data.to_dataframe().reset_index()
    df = df.rename(columns={"latitude": "lat", "longitude": "lng"})
    if "time" in df.columns:
        ts = pd.to_datetime(df["time"])
        df["year"] = ts.dt.year
        df["month"] = ts.dt.month
    df.to_parquet(out_parquet, index=False)

    day_count = (pd.to_datetime(df["time"]).dt.date.max() - pd.to_datetime(df["time"]).dt.date.min()).days + 1
    expected_hours = max(24 * day_count, 1)
    unique_hours = pd.to_datetime(ds["time"].values).size if "time" in ds else 0
    missing_ratio = max(0.0, (expected_hours - unique_hours) / expected_hours)
    nan_ratio_per_var: dict[str, float] = {}
    for var in ["2m_temperature", "total_precipitation", "10m_u_component_of_wind", "10m_v_component_of_wind", "volumetric_soil_water_layer_1"]:
        if var in ds.data_vars:
            arr = ds[var].values
            nan_ratio_per_var[var] = float(pd.isna(arr).mean())
    dq_report = {
        "expected_hours": int(expected_hours),
        "actual_hours": int(unique_hours),
        "missing_ratio": float(missing_ratio),
        "nan_ratio_per_var": nan_ratio_per_var,
        "sanity": {
            "temp_c_min": float(df["temp_mean"].min()) if "temp_mean" in df else None,
            "temp_c_max": float(df["temp_max"].max()) if "temp_max" in df else None,
            "precip_min": float(df["precip_sum"].min()) if "precip_sum" in df else None,
        },
        "file_size_bytes": int(nc_path.stat().st_size),
    }
    ds.close()
    return DailyFeatureBuildResult(rows=len(df), dq_report=dq_report)


def _save_artifact(
    db: Session,
    *,
    req_sig: str,
    job_id: str,
    artifact_type: str,
    req: Era5Request,
    gcs_uri: str,
    checksum: str,
    byte_size: int,
) -> None:
    db.add(
        Era5ArtifactORM(
            request_signature=req_sig,
            job_id=job_id,
            artifact_type=artifact_type,
            dataset=req.dataset,
            variables_csv=",".join(req.variables),
            bbox_csv=",".join(str(x) for x in req.bbox),
            start_date=req.start_date,
            end_date=req.end_date,
            gcs_uri=gcs_uri,
            checksum_sha256=checksum,
            byte_size=byte_size,
        )
    )


def submit_era5_job(req: Era5Request, *, enforce_limit: bool = True) -> tuple[str, bool]:
    req_sig = request_signature(req)
    with SessionLocal() as db:
        now = datetime.now(timezone.utc)
        stale_before = now - timedelta(minutes=30)
        stale_jobs = db.execute(
            select(Era5IngestJobORM).where(
                Era5IngestJobORM.status.in_(["queued", "running"]),
                Era5IngestJobORM.created_at < stale_before,
            )
        ).scalars().all()
        for sj in stale_jobs:
            sj.status = "failed"
            sj.finished_at = now
            sj.error = "stale job auto-recovered"
        if stale_jobs:
            db.commit()

        existing_job = db.execute(
            select(Era5IngestJobORM)
            .where(
                Era5IngestJobORM.request_signature == req_sig,
                Era5IngestJobORM.status.in_(["queued", "running", "success"]),
            )
            .order_by(desc(Era5IngestJobORM.created_at))
            .limit(1)
        ).scalar_one_or_none()
        if existing_job:
            return existing_job.job_id, True

        if enforce_limit:
            running_count = db.execute(
                select(Era5IngestJobORM).where(Era5IngestJobORM.status.in_(["queued", "running"]))
            ).scalars().all()
            if len(running_count) >= settings.era5_max_concurrent_jobs:
                raise RuntimeError("ERA5 job concurrency limit reached")

        job_id = str(uuid4())
        db.add(
            Era5IngestJobORM(
                job_id=job_id,
                request_signature=req_sig,
                status="queued",
                dataset=req.dataset,
                variables_csv=",".join(req.variables),
                bbox_csv=",".join(str(x) for x in req.bbox),
                provider=req.provider,
                mode=req.mode,
                points_set=req.points_set,
                month_label=req.month_label,
                source_range_json=req.source_range_json,
                start_date=req.start_date,
                end_date=req.end_date,
            )
        )
        db.commit()
        return job_id, False


def process_era5_job(job_id: str) -> None:
    final_status = "running"
    provider = "cds"
    with SessionLocal() as db:
        job = db.get(Era5IngestJobORM, job_id)
        if not job:
            return
        provider = job.provider or "cds"
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        db.commit()

    if provider == "aws_nsf_ncar":
        from pipeline.aws_era5_ingestion import process_aws_era5_job

        try:
            process_aws_era5_job(job_id)
        finally:
            kick_queued_jobs()
        return

    with SessionLocal() as db:
        job = db.get(Era5IngestJobORM, job_id)
        req = Era5Request(
            start_date=job.start_date,
            end_date=job.end_date,
            bbox=tuple(float(v) for v in job.bbox_csv.split(",")),  # type: ignore[arg-type]
            variables=[v.strip() for v in job.variables_csv.split(",") if v.strip()],
            dataset=job.dataset,
            out_format="netcdf",
            provider=job.provider or "cds",
            mode=job.mode or "bbox",
            points_set=job.points_set,
            month_label=job.month_label,
            source_range_json=job.source_range_json,
        )
        req_sig = job.request_signature

    total_bytes = 0
    total_rows = 0
    raw_files = 0
    feature_files = 0
    error = None
    dq_reports: list[dict] = []

    try:
        LOG.info(json.dumps({"event": "era5_job_start", "job_id": job_id, "request_signature": req_sig}))
        for year, month, days in _month_chunks(req.start_date, req.end_date):
            nc_path = Path(tempfile.gettempdir()) / f"orion_era5_{job_id}_{year}_{month:02d}.nc"
            pq_path = Path(tempfile.gettempdir()) / f"orion_era5_{job_id}_{year}_{month:02d}.parquet"

            _retrieve_month(req, year, month, days, nc_path)
            nc_checksum = _file_sha256(nc_path)
            raw_uri, raw_size = _upload_to_gcs(
                nc_path,
                f"raw/era5_land/{year}/{month:02d}/{job_id}.nc",
            )
            LOG.info(
                json.dumps(
                    {
                        "event": "era5_raw_uploaded",
                        "job_id": job_id,
                        "year": year,
                        "month": month,
                        "raw_uri": raw_uri,
                        "raw_size_bytes": raw_size,
                    }
                )
            )
            total_bytes += raw_size
            raw_files += 1

            build = _build_daily_features(nc_path, pq_path)
            rows = build.rows
            dq_reports.append(build.dq_report)
            pq_checksum = _file_sha256(pq_path)
            feat_uri, feat_size = _upload_to_gcs(
                pq_path,
                f"features/era5_land/daily/year={year}/month={month:02d}/{job_id}.parquet",
            )
            LOG.info(
                json.dumps(
                    {
                        "event": "era5_features_uploaded",
                        "job_id": job_id,
                        "year": year,
                        "month": month,
                        "feature_uri": feat_uri,
                        "feature_size_bytes": feat_size,
                        "rows_written_month": rows,
                    }
                )
            )
            total_bytes += feat_size
            total_rows += rows
            feature_files += 1

            with SessionLocal() as db:
                j = db.get(Era5IngestJobORM, job_id)
                if j:
                    j.rows_written = total_rows
                    j.bytes_downloaded = total_bytes
                    j.raw_files = raw_files
                    j.feature_files = feature_files
                    db.commit()

            with SessionLocal() as db:
                _save_artifact(
                    db,
                    req_sig=req_sig,
                    job_id=job_id,
                    artifact_type="raw_nc",
                    req=req,
                    gcs_uri=raw_uri,
                    checksum=nc_checksum,
                    byte_size=raw_size,
                )
                _save_artifact(
                    db,
                    req_sig=req_sig,
                    job_id=job_id,
                    artifact_type="feature_daily_parquet",
                    req=req,
                    gcs_uri=feat_uri,
                    checksum=pq_checksum,
                    byte_size=feat_size,
                )
                db.commit()

    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        LOG.exception("era5_job_failed job_id=%s request_signature=%s error=%s", job_id, req_sig, error)

    with SessionLocal() as db:
        job = db.get(Era5IngestJobORM, job_id)
        if not job:
            return
        job.finished_at = datetime.now(timezone.utc)
        job.rows_written = total_rows
        job.bytes_downloaded = total_bytes
        job.raw_files = raw_files
        job.feature_files = feature_files
        job.error = error
        if error:
            job.status = "failed"
        else:
            dq_status = "pass"
            for report in dq_reports:
                sanity = report.get("sanity", {})
                if (
                    (sanity.get("temp_c_min") is not None and sanity["temp_c_min"] < -80)
                    or (sanity.get("temp_c_max") is not None and sanity["temp_c_max"] > 60)
                    or (sanity.get("precip_min") is not None and sanity["precip_min"] < 0)
                    or report.get("missing_ratio", 0) > 0.2
                ):
                    dq_status = "fail"
                    break
                if report.get("missing_ratio", 0) > 0.05:
                    dq_status = "warn"
            job.dq_status = dq_status
            job.dq_report_json = json.dumps(dq_reports)
            if dq_status == "fail":
                job.status = "fail_dq"
            elif dq_status == "warn":
                job.status = "success_with_warnings"
            else:
                job.status = "success"
            final_status = job.status
        if job.started_at and job.finished_at:
            job.duration_seconds = (job.finished_at - job.started_at).total_seconds()
        db.commit()
    LOG.info(
        json.dumps(
            {
                "event": "era5_job_finish",
                "job_id": job_id,
                "status": final_status,
                "rows_written": total_rows,
                "bytes_downloaded": total_bytes,
                "raw_files": raw_files,
                "feature_files": feature_files,
                "dq_status": (dq_reports[-1].get("dq_status") if dq_reports else None),
            }
        )
    )
    kick_queued_jobs()


def get_job(job_id: str) -> Era5IngestJobORM | None:
    with SessionLocal() as db:
        return db.get(Era5IngestJobORM, job_id)


def list_feature_artifacts(start_date: date, end_date: date) -> list[Era5ArtifactORM]:
    with SessionLocal() as db:
        rows = db.execute(
            select(Era5ArtifactORM)
            .where(
                Era5ArtifactORM.artifact_type == "feature_daily_parquet",
                Era5ArtifactORM.start_date <= end_date,
                Era5ArtifactORM.end_date >= start_date,
            )
            .order_by(desc(Era5ArtifactORM.created_at))
        ).scalars().all()
        return rows


def get_era5_timeseries(lat: float, lng: float, start_date: date, end_date: date) -> list[dict]:
    artifacts = list_feature_artifacts(start_date, end_date)
    if not artifacts:
        return []

    frames: list[pd.DataFrame] = []
    seen_uris: set[str] = set()
    for art in artifacts:
        if art.gcs_uri in seen_uris:
            continue
        seen_uris.add(art.gcs_uri)
        local_path = _download_gcs_uri(art.gcs_uri)
        df = pd.read_parquet(local_path)
        if df.empty:
            continue
        nearest = _cached_nearest_coord(art.gcs_uri, round(lat, 3), round(lng, 3))
        if not nearest:
            continue
        lat0, lng0 = nearest
        sub = df[(df["lat"] == lat0) & (df["lng"] == lng0)].copy()
        sub["date"] = pd.to_datetime(sub["time"]).dt.date
        sub = sub[(sub["date"] >= start_date) & (sub["date"] <= end_date)]
        if sub.empty:
            continue
        cols = ["date", "temp_mean", "temp_max", "precip_sum", "wind_max", "soil_moisture_mean"]
        for c in cols:
            if c not in sub.columns:
                sub[c] = None
        sub["source"] = "era5"
        frames.append(sub[cols + ["source"]])

    if not frames:
        return []
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["date"]).sort_values("date")
    return [
        {
            "date": r["date"].isoformat(),
            "temp_mean": float(r["temp_mean"]) if pd.notna(r["temp_mean"]) else None,
            "temp_max": float(r["temp_max"]) if pd.notna(r["temp_max"]) else None,
            "precip_sum": float(r["precip_sum"]) if pd.notna(r["precip_sum"]) else None,
            "wind_max": float(r["wind_max"]) if pd.notna(r["wind_max"]) else None,
            "soil_moisture_mean": (
                float(r["soil_moisture_mean"]) if "soil_moisture_mean" in r and pd.notna(r["soil_moisture_mean"]) else None
            ),
            "source": r["source"],
        }
        for _, r in out.iterrows()
    ]


def get_era5_features(lat: float, lng: float, start_date: date, end_date: date) -> list[dict]:
    # Public alias matching API/roadmap wording.
    return get_era5_timeseries(lat, lng, start_date, end_date)


def _iter_month_ranges(start_month: str, end_month: str) -> list[tuple[str, date, date]]:
    sy, sm = [int(x) for x in start_month.split("-")]
    ey, em = [int(x) for x in end_month.split("-")]
    cur_y, cur_m = sy, sm
    out: list[tuple[str, date, date]] = []
    while (cur_y, cur_m) <= (ey, em):
        month_label = f"{cur_y:04d}-{cur_m:02d}"
        s = date(cur_y, cur_m, 1)
        e = date(cur_y, cur_m, monthrange(cur_y, cur_m)[1])
        out.append((month_label, s, e))
        if cur_m == 12:
            cur_y += 1
            cur_m = 1
        else:
            cur_m += 1
    return out


def submit_backfill(
    start_month: str,
    end_month: str,
    bbox: tuple[float, float, float, float],
    variables: list[str],
    mode: str,
    dataset: str,
    concurrency: int = 2,
    provider_strategy: str = "aws_first_hybrid",
    force: bool = False,
) -> tuple[str, bool, int]:
    req_sig = sha256(
        json.dumps(
            {
                "start_month": start_month,
                "end_month": end_month,
                "bbox": [round(x, 4) for x in bbox],
                "variables": sorted(variables),
                "mode": mode,
                "dataset": dataset,
                "concurrency": concurrency,
                "provider_strategy": provider_strategy,
                "force": force,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    with SessionLocal() as db:
        existing = db.execute(
            select(Era5BackfillJobORM).where(Era5BackfillJobORM.request_signature == req_sig).limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing.backfill_id, True, existing.months_total

        backfill_id = str(uuid4())
        ranges = _iter_month_ranges(start_month, end_month)
        bf = Era5BackfillJobORM(
            backfill_id=backfill_id,
            request_signature=req_sig,
            status="running",
            mode=mode,
            dataset=dataset,
            variables_csv=",".join(variables),
            bbox_csv=",".join(str(x) for x in bbox),
            start_month=start_month,
            end_month=end_month,
            months_total=len(ranges),
            months_success=0,
            months_failed=0,
            provider_strategy=provider_strategy,
            force=force,
            failed_months_json="[]",
        )
        db.add(bf)
        # Commit parent row first so child inserts never hit FK races/order issues.
        db.commit()
        if provider_strategy == "aws_first_hybrid":
            from pipeline.aws_era5_resolver import resolve_months_provider

            try:
                decisions = {
                    d.month_label: d
                    for d in resolve_months_provider(start_month=start_month, end_month=end_month, variables=variables)
                }
            except Exception as exc:  # noqa: BLE001
                LOG.warning("aws_resolver_unavailable fallback_to_cds error=%s", str(exc))
                decisions = {}
        else:
            decisions = {}

        for month_label, s, e in ranges:
            decision = decisions.get(month_label)
            provider_selected = decision.provider if decision else "cds"
            job_id: str | None = None
            job_status = "queued"
            with SessionLocal() as jdb:
                done = jdb.execute(
                    select(Era5IngestJobORM)
                    .where(
                        Era5IngestJobORM.provider
                        == ("aws_nsf_ncar" if provider_selected == "aws" else "cds"),
                        Era5IngestJobORM.dataset == dataset,
                        Era5IngestJobORM.start_date == s,
                        Era5IngestJobORM.end_date == e,
                        Era5IngestJobORM.bbox_csv == ",".join(str(x) for x in bbox),
                        Era5IngestJobORM.variables_csv == ",".join(variables),
                        Era5IngestJobORM.status.in_(["success", "success_with_warnings"]),
                        Era5IngestJobORM.raw_files > 0,
                        Era5IngestJobORM.feature_files > 0,
                    )
                    .order_by(desc(Era5IngestJobORM.created_at))
                    .limit(1)
                ).scalar_one_or_none()
                if done:
                    job_id = done.job_id
                    job_status = done.status
            if job_id is None:
                req = Era5Request(
                    start_date=s,
                    end_date=e,
                    bbox=bbox,
                    variables=variables,
                    dataset=dataset,
                    out_format="netcdf",
                    provider="aws_nsf_ncar" if provider_selected == "aws" else "cds",
                    mode=settings.aws_era5_mode_default if provider_selected == "aws" else "bbox",
                    points_set=settings.aws_era5_points_set_default if provider_selected == "aws" else None,
                    month_label=month_label,
                    source_range_json=json.dumps(
                        {
                            "provider_strategy": provider_strategy,
                            "provider_selected": provider_selected,
                            "reason": decision.reason if decision else "explicit_cds",
                        }
                    ),
                )
                job_id, _ = submit_era5_job(req, enforce_limit=False)
                with SessionLocal() as jdb:
                    ingest = jdb.get(Era5IngestJobORM, job_id)
                    if ingest and ingest.status:
                        job_status = ingest.status
            db.add(
                Era5BackfillItemORM(
                    backfill_id=backfill_id,
                    month_label=month_label,
                    start_date=s,
                    end_date=e,
                    job_id=job_id,
                    status=job_status,
                    provider_selected=provider_selected,
                    attempt_count=0,
                )
            )
        db.commit()
        kick_queued_jobs()
        return backfill_id, False, len(ranges)


def get_backfill_status(backfill_id: str, include_items: bool = True) -> dict | None:
    with SessionLocal() as db:
        bf = db.get(Era5BackfillJobORM, backfill_id)
        if not bf:
            return None
        items = db.execute(
            select(Era5BackfillItemORM).where(Era5BackfillItemORM.backfill_id == backfill_id).order_by(Era5BackfillItemORM.month_label)
        ).scalars().all()
        success = 0
        failed: list[str] = []
        all_done = True
        for item in items:
            if item.job_id:
                j = db.get(Era5IngestJobORM, item.job_id)
                if j:
                    item.status = j.status
                    item.error = j.error
                    item.finished_at = j.finished_at
                    if j.status in {"success", "success_with_warnings"}:
                        success += 1
                    elif j.status in {"failed", "fail_dq"}:
                        failed.append(item.month_label)
                    else:
                        all_done = False
                else:
                    all_done = False
            else:
                all_done = False
        bf.months_success = success
        bf.months_failed = len(failed)
        bf.failed_months_json = json.dumps(failed)
        if all_done:
            bf.status = "failed" if failed else "success"
            bf.finished_at = datetime.now(timezone.utc)
        db.commit()
        child_jobs = None
        if include_items:
            child_jobs = [
                {
                    "month": it.month_label,
                    "job_id": it.job_id,
                    "status": it.status,
                    "provider_selected": it.provider_selected,
                    "error": it.error,
                    "finished_at": it.finished_at,
                }
                for it in items
            ]
        return {
            "status": bf.status,
            "backfill_id": bf.backfill_id,
            "provider_strategy": bf.provider_strategy,
            "start_month": bf.start_month,
            "end_month": bf.end_month,
            "months_total": bf.months_total,
            "months_success": bf.months_success,
            "months_failed": bf.months_failed,
            "failed_months": failed,
            "child_jobs": child_jobs,
            "created_at": bf.created_at,
            "finished_at": bf.finished_at,
        }


def get_jobs_metrics(hours: int = 24) -> dict:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    with SessionLocal() as db:
        rows = db.execute(select(Era5IngestJobORM).where(Era5IngestJobORM.created_at >= since)).scalars().all()
    total = len(rows)
    if total == 0:
        return {"jobs_last_24h": 0, "success_rate": 0.0, "avg_duration": 0.0, "bytes_downloaded": 0}
    success = sum(1 for r in rows if r.status in {"success", "success_with_warnings"})
    avg_duration = sum((r.duration_seconds or 0.0) for r in rows) / total
    bytes_total = int(sum(r.bytes_downloaded for r in rows))
    return {
        "jobs_last_24h": total,
        "success_rate": round(success / total, 4),
        "avg_duration": round(avg_duration, 2),
        "bytes_downloaded": bytes_total,
    }


def save_export_job(job: ExportJobORM) -> None:
    with SessionLocal() as db:
        db.add(job)
        db.commit()


def start_era5_background_job(job_id: str) -> None:
    t = threading.Thread(target=process_era5_job, args=(job_id,), daemon=False)
    t.start()


def kick_queued_jobs() -> int:
    started = 0
    with SessionLocal() as db:
        running = db.execute(select(Era5IngestJobORM).where(Era5IngestJobORM.status == "running")).scalars().all()
        slots = max(0, settings.era5_max_concurrent_jobs - len(running))
        if slots == 0:
            return 0
        queued = db.execute(
            select(Era5IngestJobORM)
            .where(Era5IngestJobORM.status == "queued")
            .order_by(Era5IngestJobORM.created_at)
            .limit(slots)
        ).scalars().all()
        for q in queued:
            q.status = "running"
            q.started_at = datetime.now(timezone.utc)
        db.commit()
        for q in queued:
            t = threading.Thread(target=process_era5_job, args=(q.job_id,), daemon=False)
            t.start()
            started += 1
    return started
