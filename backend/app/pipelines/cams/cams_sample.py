from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import select
import xarray as xr

from app.database import SessionLocal
from app.orm import ProvinceORM


@dataclass(frozen=True)
class ProvincePoint:
    province_id: str
    lat: float
    lon: float


def load_province_points() -> list[ProvincePoint]:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM.id, ProvinceORM.lat, ProvinceORM.lng).order_by(ProvinceORM.plate)).all()
    return [ProvincePoint(province_id=str(pid), lat=float(lat), lon=float(lon)) for pid, lat, lon in rows]


def compute_air_quality_index(pm25, no2, o3):
    return (pm25 + no2 + o3) / 3.0


def sample_daily_to_provinces(
    *,
    daily_grid: xr.Dataset,
    provinces: list[ProvincePoint],
    source: str,
    run_id: str,
    ingested_at: datetime | None = None,
) -> pd.DataFrame:
    if not provinces:
        raise RuntimeError("No provinces found for CAMS sampling")

    if "time" not in daily_grid.coords:
        raise RuntimeError("CAMS daily grid is missing time coordinate")

    lats = np.array([p.lat for p in provinces], dtype=np.float64)
    lons = np.array([p.lon for p in provinces], dtype=np.float64)
    ids = np.array([p.province_id for p in provinces], dtype=object)

    sampled = daily_grid.sel(
        latitude=xr.DataArray(lats, dims="province"),
        longitude=xr.DataArray(lons, dims="province"),
        method="nearest",
    )
    sampled = sampled.transpose("time", "province")

    times = pd.to_datetime(sampled["time"].values, utc=True, errors="coerce")
    valid_mask = ~pd.isna(times)
    times = times[valid_mask]
    if len(times) == 0:
        return pd.DataFrame(
            columns=[
                "date",
                "province_id",
                "lat",
                "lon",
                "pm25_mean_ugm3",
                "no2_mean_ugm3",
                "o3_mean_ugm3",
                "air_quality_index",
                "source",
                "run_id",
                "ingested_at",
            ]
        )

    pm25 = np.asarray(sampled["pm25_mean_ugm3"].values, dtype=np.float64)[valid_mask, :]
    no2 = np.asarray(sampled["no2_mean_ugm3"].values, dtype=np.float64)[valid_mask, :]
    o3 = np.asarray(sampled["o3_mean_ugm3"].values, dtype=np.float64)[valid_mask, :]

    rows = len(times)
    cols = len(provinces)
    out = pd.DataFrame(
        {
            "date": np.repeat(times.date, cols),
            "province_id": np.tile(ids, rows),
            "lat": np.tile(lats, rows),
            "lon": np.tile(lons, rows),
            "pm25_mean_ugm3": pm25.reshape(rows * cols),
            "no2_mean_ugm3": no2.reshape(rows * cols),
            "o3_mean_ugm3": o3.reshape(rows * cols),
        }
    )
    out["air_quality_index"] = compute_air_quality_index(out["pm25_mean_ugm3"], out["no2_mean_ugm3"], out["o3_mean_ugm3"])
    out["source"] = source
    out["run_id"] = run_id
    out["ingested_at"] = ingested_at or datetime.now(timezone.utc)

    return out[
        [
            "date",
            "province_id",
            "lat",
            "lon",
            "pm25_mean_ugm3",
            "no2_mean_ugm3",
            "o3_mean_ugm3",
            "air_quality_index",
            "source",
            "run_id",
            "ingested_at",
        ]
    ]
