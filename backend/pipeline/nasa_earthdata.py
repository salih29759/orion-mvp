from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import logging
from pathlib import Path
import re
import tempfile
import threading
from typing import Any
from uuid import uuid4

from google.cloud import storage
import h5py
import numpy as np
import pandas as pd
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import NasaIngestJobORM, ProvinceORM

LOG = logging.getLogger("orion.nasa")

TURKEY_BBOX_WGS84 = (26.0, 36.0, 45.0, 42.0)  # west, south, east, north
SMAP_SHORT_NAME = "SPL3SMP"
SMAP_VERSION = "008"
SMAP_SOURCE = "smap_spl3smp_v008"
SMAP_MIN_DATE = date(2015, 4, 1)

MODIS_SHORT_NAME = "MCD64A1"
MODIS_VERSION = "061"
MODIS_SOURCE = "modis_mcd64a1_v061"
MODIS_MIN_DATE = date(2000, 11, 1)

MODIS_PIXEL_SIZE_M = 463.31271653
MODIS_TILE_SIZE = 2400
MODIS_SIN_RADIUS = 6371007.181
MODIS_X_MIN = -20015109.354
MODIS_Y_MAX = 10007554.677
MODIS_PIXEL_AREA_KM2 = (MODIS_PIXEL_SIZE_M / 1000.0) ** 2

_NASA_ACTIVE_OR_SUCCESS = {"queued", "running", "success", "success_with_warnings"}
_NASA_ACTIVE = {"queued", "running"}


def _nasa_type(dataset: str) -> str:
    return f"nasa_{dataset}_backfill"


def _earthaccess_module():
    try:
        import earthaccess
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("earthaccess is required for NASA ingestion") from exc
    return earthaccess


def _earthaccess_login():
    earthaccess = _earthaccess_module()
    earthaccess.login(strategy="netrc")
    return earthaccess


def _iter_month_chunks(start_date: date, end_date: date) -> list[tuple[int, int, date, date]]:
    chunks: list[tuple[int, int, date, date]] = []
    cur = date(start_date.year, start_date.month, 1)
    while cur <= end_date:
        month_end = date(cur.year, cur.month, monthrange(cur.year, cur.month)[1])
        s = max(start_date, cur)
        e = min(end_date, month_end)
        chunks.append((cur.year, cur.month, s, e))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return chunks


def _normalize_range(dataset: str, start_date: date, end_date: date) -> tuple[date, date]:
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    today = datetime.now(timezone.utc).date()
    if dataset == "smap":
        if start_date < SMAP_MIN_DATE:
            raise ValueError(f"SMAP start_date must be >= {SMAP_MIN_DATE.isoformat()}")
        effective_end = min(end_date, today)
    elif dataset == "modis":
        if start_date < MODIS_MIN_DATE:
            raise ValueError(f"MODIS start_date must be >= {MODIS_MIN_DATE.isoformat()}")
        first_of_month = date(today.year, today.month, 1)
        last_complete_month_day = first_of_month - timedelta(days=1)
        effective_end = min(end_date, last_complete_month_day)
    else:
        raise ValueError(f"Unsupported NASA dataset '{dataset}'")

    if effective_end < start_date:
        raise ValueError("Requested period has no available data yet")

    return start_date, effective_end


def _request_signature(dataset: str, start_date: date, end_date: date) -> str:
    payload = {
        "dataset": dataset,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "bbox": [round(v, 4) for v in TURKEY_BBOX_WGS84],
        "version": SMAP_VERSION if dataset == "smap" else MODIS_VERSION,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _storage_client() -> storage.Client:
    return storage.Client()


def _load_province_points() -> pd.DataFrame:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM.plate, ProvinceORM.lat, ProvinceORM.lng).order_by(ProvinceORM.plate)).all()
    if not rows:
        raise RuntimeError("No provinces found in DB")

    out = pd.DataFrame(
        [
            {
                "point_id": f"province:{int(plate):02d}",
                "lat": float(lat),
                "lon": float(lon),
            }
            for plate, lat, lon in rows
        ]
    )
    return out


def _upload_partition(frame: pd.DataFrame, object_name: str) -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    local = Path(tempfile.gettempdir()) / f"orion_nasa_{uuid4().hex}.parquet"
    frame.to_parquet(local, index=False)

    client = _storage_client()
    bucket = client.bucket(settings.era5_gcs_bucket)
    bucket.blob(object_name).upload_from_filename(str(local))
    return f"gs://{settings.era5_gcs_bucket}/{object_name}"


def decode_smap_retrieval_flag(raw_flag: Any) -> int:
    # Retrieval quality is bit-packed in SPL3SMP; map to API contract enum.
    if raw_flag is None:
        return 2
    try:
        if pd.isna(raw_flag):
            return 2
        ival = int(raw_flag)
    except Exception:  # noqa: BLE001
        return 2

    if ival < 0:
        return 2
    if ival == 0:
        return 0

    retrieved = bool(ival & 1)
    recommended = ((ival >> 1) & 1) == 0
    if retrieved and recommended:
        return 0
    if retrieved:
        return 1
    return 2


def modis_land_valid_mask(qa_arr: np.ndarray) -> np.ndarray:
    qa = np.asarray(qa_arr, dtype=np.int64)
    return ((qa & 1) == 1) & (((qa >> 1) & 1) == 1)


def modis_tile_rowcol_to_latlon(h: int, v: int, rows: np.ndarray, cols: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    tile_span = MODIS_TILE_SIZE * MODIS_PIXEL_SIZE_M
    x0 = MODIS_X_MIN + (h * tile_span)
    y0 = MODIS_Y_MAX - (v * tile_span)

    x = x0 + (cols.astype(np.float64) + 0.5) * MODIS_PIXEL_SIZE_M
    y = y0 - (rows.astype(np.float64) + 0.5) * MODIS_PIXEL_SIZE_M

    lat_rad = y / MODIS_SIN_RADIUS
    cos_lat = np.cos(lat_rad)
    lon_rad = np.divide(
        x,
        MODIS_SIN_RADIUS * cos_lat,
        out=np.full_like(x, np.nan, dtype=np.float64),
        where=np.abs(cos_lat) > 1e-12,
    )

    lat = np.degrees(lat_rad)
    lon = np.degrees(lon_rad)
    return lat, lon


def _parse_smap_date(file_name: str) -> date | None:
    for pattern in (r"A(20\d{6})", r"(20\d{6})"):
        m = re.search(pattern, file_name)
        if not m:
            continue
        try:
            return datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            continue
    return None


def _parse_modis_tile(file_name: str) -> tuple[int, int] | None:
    m = re.search(r"[._]h(\d{2})v(\d{2})[._]", file_name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _h5_pick_dataset(hf: h5py.File, preferred_paths: list[str], fallback_suffixes: list[str]) -> np.ndarray | None:
    for path in preferred_paths:
        normalized = path.strip("/")
        if normalized in hf:
            return np.asarray(hf[normalized])

    dataset_names: list[str] = []

    def _collect(name: str, obj: Any) -> None:
        if isinstance(obj, h5py.Dataset):
            dataset_names.append(name)

    hf.visititems(_collect)

    lowered = {name.lower(): name for name in dataset_names}
    for suffix in fallback_suffixes:
        suffix_l = suffix.lower()
        am_candidates = [name for name in lowered if name.endswith(suffix_l) and "retrieval_data_am" in name]
        if am_candidates:
            return np.asarray(hf[lowered[sorted(am_candidates)[0]]])
        candidates = [name for name in lowered if name.endswith(suffix_l)]
        if candidates:
            return np.asarray(hf[lowered[sorted(candidates)[0]]])
    return None


def _smap_file_rows(file_path: Path, day: date, provinces: pd.DataFrame) -> list[dict[str, Any]]:
    with h5py.File(file_path, "r") as hf:
        soil = _h5_pick_dataset(
            hf,
            preferred_paths=[
                "Soil_Moisture_Retrieval_Data_AM/soil_moisture",
                "Soil_Moisture_Retrieval_Data_AM/soil_moisture_am",
            ],
            fallback_suffixes=["/soil_moisture", "/soil_moisture_am"],
        )
        qual = _h5_pick_dataset(
            hf,
            preferred_paths=["Soil_Moisture_Retrieval_Data_AM/retrieval_qual_flag"],
            fallback_suffixes=["/retrieval_qual_flag"],
        )
        lat = _h5_pick_dataset(
            hf,
            preferred_paths=["Soil_Moisture_Retrieval_Data_AM/latitude"],
            fallback_suffixes=["/latitude"],
        )
        lon = _h5_pick_dataset(
            hf,
            preferred_paths=["Soil_Moisture_Retrieval_Data_AM/longitude"],
            fallback_suffixes=["/longitude"],
        )

    if soil is None or qual is None or lat is None or lon is None:
        raise RuntimeError(f"SMAP file missing required AM fields: {file_path.name}")

    soil = np.asarray(soil)
    qual = np.asarray(qual)
    lat = np.asarray(lat)
    lon = np.asarray(lon)

    while soil.ndim > 2:
        soil = soil[0]
    while qual.ndim > 2:
        qual = qual[0]

    if lat.ndim == 1 and lon.ndim == 1:
        lon2d, lat2d = np.meshgrid(lon, lat)
    elif lat.shape == soil.shape and lon.shape == soil.shape:
        lat2d = lat
        lon2d = lon
    else:
        raise RuntimeError(f"SMAP coordinate shape mismatch in {file_path.name}")

    if qual.shape != soil.shape:
        raise RuntimeError(f"SMAP qual shape mismatch in {file_path.name}")

    west, south, east, north = TURKEY_BBOX_WGS84
    bbox_mask = (
        np.isfinite(lat2d)
        & np.isfinite(lon2d)
        & (lat2d >= south)
        & (lat2d <= north)
        & (lon2d >= west)
        & (lon2d <= east)
    )
    if not np.any(bbox_mask):
        return []

    lat_flat = lat2d[bbox_mask].astype(np.float64)
    lon_flat = lon2d[bbox_mask].astype(np.float64)
    soil_flat = soil[bbox_mask].astype(np.float64)
    qual_flat = qual[bbox_mask]

    prov_lats = provinces["lat"].to_numpy(dtype=np.float64)
    prov_lons = provinces["lon"].to_numpy(dtype=np.float64)
    dist = np.abs(prov_lats[:, None] - lat_flat[None, :]) + np.abs(prov_lons[:, None] - lon_flat[None, :])
    nearest_idx = np.argmin(dist, axis=1)

    rows: list[dict[str, Any]] = []
    for i, province in provinces.reset_index(drop=True).iterrows():
        idx = int(nearest_idx[i])
        soil_val = float(soil_flat[idx]) if np.isfinite(soil_flat[idx]) and soil_flat[idx] > -9000 else None
        flag = decode_smap_retrieval_flag(qual_flat[idx])
        rows.append(
            {
                "date": day,
                "point_id": province["point_id"],
                "lat": float(province["lat"]),
                "lon": float(province["lon"]),
                "soil_moisture_smap_m3m3": soil_val,
                "smap_retrieval_flag": int(flag),
            }
        )
    return rows


def _modis_read_dataset(sd: Any, names: list[str]) -> np.ndarray:
    for name in names:
        try:
            return np.asarray(sd.select(name)[:])
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError(f"Could not find MODIS SDS in candidates: {names}")


def _modis_file_pixels(file_path: Path, provinces: pd.DataFrame) -> pd.DataFrame:
    try:
        from pyhdf.SD import SD, SDC
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("pyhdf is required for MODIS burned area ingestion") from exc

    tile = _parse_modis_tile(file_path.name)
    if tile is None:
        raise RuntimeError(f"Could not parse MODIS tile from file name: {file_path.name}")
    h, v = tile

    sd = SD(str(file_path), SDC.READ)
    try:
        burn = _modis_read_dataset(sd, ["Burn Date", "BurnDate"])
        qa = _modis_read_dataset(sd, ["QA", "qa"])
    finally:
        sd.end()

    burn = np.asarray(burn, dtype=np.int32)
    qa = np.asarray(qa, dtype=np.int32)
    if burn.shape != qa.shape:
        raise RuntimeError(f"MODIS BurnDate/QA shape mismatch in {file_path.name}")

    valid = modis_land_valid_mask(qa)
    burned = (burn >= 1) & (burn <= 366)
    mask = valid & burned
    if not np.any(mask):
        return pd.DataFrame(columns=["point_id", "burn_doy"])

    rows, cols = np.where(mask)
    burn_doy = burn[rows, cols].astype(np.int32)
    lat, lon = modis_tile_rowcol_to_latlon(h, v, rows, cols)

    west, south, east, north = TURKEY_BBOX_WGS84
    bbox = (
        np.isfinite(lat)
        & np.isfinite(lon)
        & (lat >= south)
        & (lat <= north)
        & (lon >= west)
        & (lon <= east)
    )
    if not np.any(bbox):
        return pd.DataFrame(columns=["point_id", "burn_doy"])

    lat = lat[bbox]
    lon = lon[bbox]
    burn_doy = burn_doy[bbox]

    prov_lats = provinces["lat"].to_numpy(dtype=np.float64)
    prov_lons = provinces["lon"].to_numpy(dtype=np.float64)
    dist = np.abs(lat[:, None] - prov_lats[None, :]) + np.abs(lon[:, None] - prov_lons[None, :])
    nearest_idx = np.argmin(dist, axis=1)
    point_ids = provinces["point_id"].to_numpy(dtype=object)[nearest_idx]

    return pd.DataFrame({"point_id": point_ids, "burn_doy": burn_doy})


def _search_and_download(
    earthaccess: Any,
    *,
    short_name: str,
    version: str,
    start_date: date,
    end_date: date,
    target_dir: Path,
) -> list[Path]:
    results = earthaccess.search_data(
        short_name=short_name,
        version=version,
        temporal=(start_date.isoformat(), end_date.isoformat()),
        bounding_box=TURKEY_BBOX_WGS84,
    )
    if not results:
        return []

    downloaded = earthaccess.download(results, str(target_dir)) or []
    files: list[Path] = []
    for item in downloaded:
        p = Path(str(item))
        if p.is_file():
            files.append(p)

    if not files:
        files = [p for p in target_dir.rglob("*") if p.is_file()]

    unique_files = sorted({str(p): p for p in files}.values())
    return unique_files


def _process_smap_month(
    *,
    earthaccess: Any,
    year: int,
    month: int,
    month_start: date,
    month_end: date,
    provinces: pd.DataFrame,
    run_id: str,
) -> dict[str, Any]:
    tmp_dir = Path(tempfile.gettempdir()) / f"orion_smap_{run_id}_{year:04d}_{month:02d}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    files = _search_and_download(
        earthaccess,
        short_name=SMAP_SHORT_NAME,
        version=SMAP_VERSION,
        start_date=month_start,
        end_date=month_end,
        target_dir=tmp_dir,
    )
    if not files:
        raise RuntimeError(f"No SMAP granules for {year:04d}-{month:02d}")

    rows: list[dict[str, Any]] = []
    for file_path in files:
        day = _parse_smap_date(file_path.name)
        if day is None:
            LOG.warning("Skipping SMAP file with unknown date format: %s", file_path.name)
            continue
        if day < month_start or day > month_end:
            continue
        rows.extend(_smap_file_rows(file_path, day, provinces))

    if not rows:
        raise RuntimeError(f"SMAP parsing produced no rows for {year:04d}-{month:02d}")

    frame = pd.DataFrame(rows)
    frame = (
        frame.groupby(["date", "point_id", "lat", "lon"], as_index=False)
        .agg(
            soil_moisture_smap_m3m3=("soil_moisture_smap_m3m3", "mean"),
            smap_retrieval_flag=("smap_retrieval_flag", "min"),
        )
        .sort_values(["date", "point_id"])
    )
    frame["source"] = SMAP_SOURCE
    frame["run_id"] = run_id
    frame["ingested_at"] = datetime.now(timezone.utc)

    object_name = f"features/daily/smap/year={year:04d}/month={month:02d}/part-0.parquet"
    output_uri = _upload_partition(frame, object_name)

    return {
        "rows_written": int(len(frame)),
        "files_downloaded": len(files),
        "files_written": 1,
        "output_uri": output_uri,
    }


def _process_modis_month(
    *,
    earthaccess: Any,
    year: int,
    month: int,
    month_start: date,
    month_end: date,
    provinces: pd.DataFrame,
    run_id: str,
) -> dict[str, Any]:
    tmp_dir = Path(tempfile.gettempdir()) / f"orion_modis_{run_id}_{year:04d}_{month:02d}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    files = _search_and_download(
        earthaccess,
        short_name=MODIS_SHORT_NAME,
        version=MODIS_VERSION,
        start_date=month_start,
        end_date=month_end,
        target_dir=tmp_dir,
    )
    if not files:
        raise RuntimeError(f"No MODIS granules for {year:04d}-{month:02d}")

    agg: dict[str, dict[str, Any]] = {}
    for file_path in files:
        pix = _modis_file_pixels(file_path, provinces)
        if pix.empty:
            continue
        for point_id, grp in pix.groupby("point_id"):
            state = agg.setdefault(
                str(point_id),
                {
                    "burned_pixels": 0,
                    "burn_doys": set(),
                    "latest_burn_doy": None,
                },
            )
            vals = [int(v) for v in grp["burn_doy"].tolist() if 1 <= int(v) <= 366]
            if not vals:
                continue
            state["burned_pixels"] += len(vals)
            state["burn_doys"].update(vals)
            latest = max(vals)
            if state["latest_burn_doy"] is None or latest > state["latest_burn_doy"]:
                state["latest_burn_doy"] = latest

    now = datetime.now(timezone.utc)
    records: list[dict[str, Any]] = []
    for province in provinces.to_dict("records"):
        state = agg.get(str(province["point_id"]))
        burned_pixels = int(state["burned_pixels"]) if state else 0
        burn_events = len(state["burn_doys"]) if state else 0
        latest = int(state["latest_burn_doy"]) if state and state["latest_burn_doy"] is not None else None
        records.append(
            {
                "year": year,
                "month": month,
                "point_id": province["point_id"],
                "lat": float(province["lat"]),
                "lon": float(province["lon"]),
                "burned_area_km2": float(burned_pixels * MODIS_PIXEL_AREA_KM2),
                "burn_events_count": int(burn_events),
                "latest_burn_doy": latest,
                "source": MODIS_SOURCE,
                "run_id": run_id,
                "ingested_at": now,
            }
        )

    frame = pd.DataFrame(records).sort_values(["point_id"])
    object_name = f"features/monthly/modis_burned/year={year:04d}/month={month:02d}/part-0.parquet"
    output_uri = _upload_partition(frame, object_name)

    return {
        "rows_written": int(len(frame)),
        "files_downloaded": len(files),
        "files_written": 1,
        "output_uri": output_uri,
    }


def _persist_progress(
    job_id: str,
    *,
    months_completed: int,
    months_failed: int,
    rows_written: int,
    files_downloaded: int,
    files_written: int,
    progress_payload: dict[str, Any],
) -> None:
    with SessionLocal() as db:
        job = db.get(NasaIngestJobORM, job_id)
        if not job:
            return
        job.months_completed = months_completed
        job.months_failed = months_failed
        job.rows_written = rows_written
        job.files_downloaded = files_downloaded
        job.files_written = files_written
        job.progress_json = json.dumps(progress_payload)
        db.commit()


def process_nasa_job(job_id: str) -> None:
    with SessionLocal() as db:
        job = db.get(NasaIngestJobORM, job_id)
        if not job:
            return
        dataset = str(job.dataset)
        start_date = job.start_date
        end_date = job.end_date
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        db.commit()

    months = _iter_month_chunks(start_date, end_date)
    progress_payload: dict[str, Any] = {
        "dataset": dataset,
        "bbox": list(TURKEY_BBOX_WGS84),
        "months": [],
    }

    months_completed = 0
    months_failed = 0
    rows_written = 0
    files_downloaded = 0
    files_written = 0

    try:
        provinces = _load_province_points()
        earthaccess = _earthaccess_login()

        for year, month, month_start, month_end in months:
            month_label = f"{year:04d}-{month:02d}"
            try:
                if dataset == "smap":
                    stats = _process_smap_month(
                        earthaccess=earthaccess,
                        year=year,
                        month=month,
                        month_start=month_start,
                        month_end=month_end,
                        provinces=provinces,
                        run_id=job_id,
                    )
                elif dataset == "modis":
                    stats = _process_modis_month(
                        earthaccess=earthaccess,
                        year=year,
                        month=month,
                        month_start=month_start,
                        month_end=month_end,
                        provinces=provinces,
                        run_id=job_id,
                    )
                else:
                    raise RuntimeError(f"Unsupported dataset '{dataset}'")

                months_completed += 1
                rows_written += int(stats.get("rows_written", 0))
                files_downloaded += int(stats.get("files_downloaded", 0))
                files_written += int(stats.get("files_written", 0))
                progress_payload["months"].append(
                    {
                        "month": month_label,
                        "status": "success",
                        "rows_written": int(stats.get("rows_written", 0)),
                        "files_downloaded": int(stats.get("files_downloaded", 0)),
                        "files_written": int(stats.get("files_written", 0)),
                        "output_uri": stats.get("output_uri"),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                months_failed += 1
                progress_payload["months"].append(
                    {
                        "month": month_label,
                        "status": "failed",
                        "error": str(exc),
                    }
                )
                LOG.exception("nasa_month_failed dataset=%s job_id=%s month=%s", dataset, job_id, month_label)

            _persist_progress(
                job_id,
                months_completed=months_completed,
                months_failed=months_failed,
                rows_written=rows_written,
                files_downloaded=files_downloaded,
                files_written=files_written,
                progress_payload=progress_payload,
            )

        if months_completed == 0:
            final_status = "failed"
            final_error = "NASA processing failed for all months"
        elif months_failed > 0:
            final_status = "success_with_warnings"
            final_error = None
        else:
            final_status = "success"
            final_error = None

    except Exception as exc:  # noqa: BLE001
        final_status = "failed"
        final_error = str(exc)
        LOG.exception("nasa_job_failed dataset=%s job_id=%s", dataset, job_id)

    with SessionLocal() as db:
        job = db.get(NasaIngestJobORM, job_id)
        if not job:
            return
        job.status = final_status
        job.months_completed = months_completed
        job.months_failed = months_failed
        job.rows_written = rows_written
        job.files_downloaded = files_downloaded
        job.files_written = files_written
        job.progress_json = json.dumps(progress_payload)
        job.error = final_error
        job.finished_at = datetime.now(timezone.utc)
        db.commit()


def start_nasa_background_job(job_id: str) -> None:
    t = threading.Thread(target=process_nasa_job, args=(job_id,), daemon=False)
    t.start()


def submit_nasa_backfill(dataset: str, *, start_date: date, end_date: date) -> tuple[str, bool, int]:
    normalized_start, normalized_end = _normalize_range(dataset, start_date, end_date)
    chunks = _iter_month_chunks(normalized_start, normalized_end)
    signature = _request_signature(dataset, normalized_start, normalized_end)

    with SessionLocal() as db:
        existing = db.execute(
            select(NasaIngestJobORM)
            .where(
                NasaIngestJobORM.request_signature == signature,
                NasaIngestJobORM.status.in_(sorted(_NASA_ACTIVE_OR_SUCCESS)),
            )
            .limit(1)
        ).scalar_one_or_none()
        if existing:
            return existing.job_id, True, int(existing.months_total)

        job_id = str(uuid4())
        db.add(
            NasaIngestJobORM(
                job_id=job_id,
                request_signature=signature,
                dataset=dataset,
                status="queued",
                start_date=normalized_start,
                end_date=normalized_end,
                months_total=len(chunks),
                months_completed=0,
                months_failed=0,
                rows_written=0,
                files_downloaded=0,
                files_written=0,
                progress_json=json.dumps({"dataset": dataset, "bbox": list(TURKEY_BBOX_WGS84), "months": []}),
            )
        )
        db.commit()

    start_nasa_background_job(job_id)
    return job_id, False, len(chunks)


def get_nasa_job(job_id: str) -> NasaIngestJobORM | None:
    with SessionLocal() as db:
        return db.get(NasaIngestJobORM, job_id)


def get_latest_nasa_job(dataset: str) -> NasaIngestJobORM | None:
    with SessionLocal() as db:
        active = db.execute(
            select(NasaIngestJobORM)
            .where(
                NasaIngestJobORM.dataset == dataset,
                NasaIngestJobORM.status.in_(sorted(_NASA_ACTIVE)),
            )
            .order_by(desc(NasaIngestJobORM.created_at))
            .limit(1)
        ).scalar_one_or_none()
        if active:
            return active

        return db.execute(
            select(NasaIngestJobORM)
            .where(NasaIngestJobORM.dataset == dataset)
            .order_by(desc(NasaIngestJobORM.created_at))
            .limit(1)
        ).scalar_one_or_none()


def get_latest_nasa_jobs() -> dict[str, NasaIngestJobORM | None]:
    return {
        "smap": get_latest_nasa_job("smap"),
        "modis": get_latest_nasa_job("modis"),
    }


def nasa_job_to_status_payload(job: NasaIngestJobORM) -> dict[str, Any]:
    progress: dict[str, Any] = {
        "months_total": int(job.months_total),
        "months_completed": int(job.months_completed),
        "months_failed": int(job.months_failed),
        "rows_written": int(job.rows_written),
        "files_downloaded": int(job.files_downloaded),
        "files_written": int(job.files_written),
    }
    if job.progress_json:
        try:
            payload = json.loads(job.progress_json)
            if isinstance(payload, dict):
                progress.update(payload)
        except json.JSONDecodeError:
            pass

    if job.error:
        progress["error"] = job.error

    return {
        "job_id": job.job_id,
        "status": job.status,
        "type": _nasa_type(job.dataset),
        "created_at": job.created_at,
        "updated_at": job.finished_at or job.started_at,
        "progress": progress,
        "children": [],
    }
