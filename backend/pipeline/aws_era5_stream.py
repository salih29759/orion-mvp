from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from app.config import settings

try:
    import s3fs
except Exception:  # noqa: BLE001
    s3fs = None  # type: ignore[assignment]

ERA5_VAR_ALIASES = {
    "t2m": "2m_temperature",
    "tp": "total_precipitation",
    "u10": "10m_u_component_of_wind",
    "v10": "10m_v_component_of_wind",
    "swvl1": "volumetric_soil_water_layer_1",
    "var_2t": "2m_temperature",
    "var_tp": "total_precipitation",
    "var_10u": "10m_u_component_of_wind",
    "var_10v": "10m_v_component_of_wind",
    "var_swvl1": "volumetric_soil_water_layer_1",
    "var_lsp": "large_scale_precipitation",
    "var_cp": "convective_precipitation",
}


@dataclass(frozen=True)
class TurkeyBBox:
    north: float = 42.0
    west: float = 26.0
    south: float = 36.0
    east: float = 45.0


def _normalize_dataset(ds: xr.Dataset) -> xr.Dataset:
    rename_map: dict[str, str] = {}
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        rename_map["valid_time"] = "time"
    if "lat" in ds.coords and "latitude" not in ds.coords:
        rename_map["lat"] = "latitude"
    if "lon" in ds.coords and "longitude" not in ds.coords:
        rename_map["lon"] = "longitude"
    for old, new in ERA5_VAR_ALIASES.items():
        for candidate in (old, old.upper()):
            if candidate in ds.data_vars and new not in ds.data_vars:
                rename_map[candidate] = new
    if rename_map:
        ds = ds.rename(rename_map)
    return ds


def _slice_turkey(ds: xr.Dataset, bbox: TurkeyBBox | None = None) -> xr.Dataset:
    bounds = bbox or TurkeyBBox()

    if "latitude" not in ds.coords or "longitude" not in ds.coords:
        raise RuntimeError("dataset is missing latitude/longitude coordinates")

    lat_values = ds["latitude"].values
    lon_values = ds["longitude"].values

    lat_slice = slice(bounds.north, bounds.south)
    if len(lat_values) >= 2 and float(lat_values[0]) < float(lat_values[-1]):
        lat_slice = slice(bounds.south, bounds.north)

    lon_slice = slice(bounds.west, bounds.east)
    if len(lon_values) >= 2 and float(lon_values[0]) > float(lon_values[-1]):
        lon_slice = slice(bounds.east, bounds.west)

    return ds.sel(latitude=lat_slice, longitude=lon_slice)


def open_era5_from_s3(s3_key: str) -> xr.Dataset:
    if s3fs is None:
        raise RuntimeError("s3fs is required for streaming mode")
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": settings.aws_era5_region})
    uri = f"s3://{settings.aws_era5_bucket}/{s3_key}"
    errors: list[str] = []

    for engine in ("h5netcdf", "netcdf4", "scipy"):
        file_obj = fs.open(uri, mode="rb")
        try:
            # Avoid dask-backed chunk graphs in VM backfill workers; they caused heavy lock contention
            # and very slow/no-progress behavior for point extraction. Read directly from backend.
            ds = xr.open_dataset(file_obj, engine=engine, chunks=None)
            ds = _normalize_dataset(ds)
            return _slice_turkey(ds)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{engine}:{exc}")
            try:
                file_obj.close()
            except Exception:  # noqa: BLE001
                pass

    raise RuntimeError(f"Unable to open {uri} as NetCDF stream ({'; '.join(errors)})")


def pick_data_var(ds: xr.Dataset) -> str:
    if not ds.data_vars:
        raise RuntimeError("dataset has no data_vars")
    if len(ds.data_vars) == 1:
        return next(iter(ds.data_vars.keys()))
    for candidate in (
        "2m_temperature",
        "total_precipitation",
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "volumetric_soil_water_layer_1",
    ):
        if candidate in ds.data_vars:
            return candidate
    return next(iter(ds.data_vars.keys()))


def extract_points_hourly(ds: xr.Dataset, points: list[dict[str, Any]], variable_name: str | None = None) -> pd.DataFrame:
    var = variable_name or pick_data_var(ds)
    da = ds[var]
    if "time" not in da.coords:
        raise RuntimeError("dataset is missing time coordinate")
    if not points:
        return pd.DataFrame(columns=["time", "point_id", "lat", "lng", "variable", "value"])

    point_ids = np.array([str(point["point_id"]) for point in points], dtype=object)
    lats = np.array([float(point["lat"]) for point in points], dtype=np.float64)
    lons = np.array([float(point["lon"]) for point in points], dtype=np.float64)

    sampled = da.sel(
        latitude=xr.DataArray(lats, dims="point"),
        longitude=xr.DataArray(lons, dims="point"),
        method="nearest",
    )
    sampled = sampled.transpose("time", "point")

    times = pd.to_datetime(sampled["time"].values, utc=True, errors="coerce")
    values = np.asarray(sampled.values, dtype=np.float64)
    if values.ndim == 1:
        values = values.reshape(-1, 1)

    valid_mask = ~pd.isna(times)
    if not valid_mask.any():
        return pd.DataFrame(columns=["time", "point_id", "lat", "lng", "variable", "value"])

    times = times[valid_mask]
    values = values[valid_mask, :]

    rows = len(times)
    cols = len(point_ids)

    out = pd.DataFrame(
        {
            "time": np.repeat(times.to_numpy(), cols),
            "point_id": np.tile(point_ids, rows),
            "lat": np.tile(lats, rows),
            "lng": np.tile(lons, rows),
            "variable": var,
            "value": values.reshape(rows * cols),
        }
    )
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return out


def map_precip_components(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    cols = set(out.columns)
    if "total_precipitation" not in cols and {"large_scale_precipitation", "convective_precipitation"}.issubset(cols):
        out["total_precipitation"] = pd.to_numeric(out["large_scale_precipitation"], errors="coerce") + pd.to_numeric(
            out["convective_precipitation"], errors="coerce"
        )
    return out


def aggregate_daily_features(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly.empty:
        return hourly

    frame = hourly.copy()
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce").dt.tz_convert(None)
    frame = frame.dropna(subset=["time"])
    frame["date"] = frame["time"].dt.date

    if "2m_temperature" in frame:
        temp = pd.to_numeric(frame["2m_temperature"], errors="coerce")
        if not temp.dropna().empty and float(temp.quantile(0.5)) > 150.0:
            temp = temp - 273.15
        frame["2m_temperature"] = temp

    if "total_precipitation" in frame:
        precip = pd.to_numeric(frame["total_precipitation"], errors="coerce")
        if not precip.dropna().empty and float(precip.quantile(0.99)) <= 5.0:
            precip = precip * 1000.0
        frame["total_precipitation"] = precip

    if "10m_u_component_of_wind" in frame and "10m_v_component_of_wind" in frame:
        u = pd.to_numeric(frame["10m_u_component_of_wind"], errors="coerce")
        v = pd.to_numeric(frame["10m_v_component_of_wind"], errors="coerce")
        frame["wind_speed"] = (u**2 + v**2) ** 0.5

    daily = (
        frame.groupby(["point_id", "lat", "lng", "date"], dropna=False)
        .agg(
            temp_mean=("2m_temperature", "mean"),
            temp_max=("2m_temperature", "max"),
            precip_sum=("total_precipitation", "sum"),
            wind_max=("wind_speed", "max"),
            soil_moisture_mean=("volumetric_soil_water_layer_1", "mean"),
        )
        .reset_index()
    )
    daily["time"] = pd.to_datetime(daily["date"])
    daily["year"] = pd.to_datetime(daily["date"]).dt.year
    daily["month"] = pd.to_datetime(daily["date"]).dt.month

    daily["temp_mean_c"] = daily["temp_mean"]
    daily["temp_max_c"] = daily["temp_max"]
    daily["precip_sum_mm"] = daily["precip_sum"]
    daily["wind_max_ms"] = daily["wind_max"]

    return daily[
        [
            "time",
            "date",
            "year",
            "month",
            "point_id",
            "lat",
            "lng",
            "temp_mean",
            "temp_max",
            "precip_sum",
            "wind_max",
            "soil_moisture_mean",
            "temp_mean_c",
            "temp_max_c",
            "precip_sum_mm",
            "wind_max_ms",
        ]
    ]
