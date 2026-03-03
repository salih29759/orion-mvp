from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import xarray as xr


@dataclass(frozen=True)
class TurkeyBBox:
    north: float = 42.0
    west: float = 26.0
    south: float = 36.0
    east: float = 45.0


PM25_ALIASES = (
    "pm2p5",
    "particulate_matter_2.5um",
    "particulate_matter_2p5um",
    "particulate_matter_d_le_2_5_um_p10",
)
NO2_ALIASES = ("no2", "nitrogen_dioxide")
O3_ALIASES = ("o3", "ozone")


def _normalize_coord_names(ds: xr.Dataset) -> xr.Dataset:
    rename_map: dict[str, str] = {}
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        rename_map["valid_time"] = "time"
    if "lat" in ds.coords and "latitude" not in ds.coords:
        rename_map["lat"] = "latitude"
    if "lon" in ds.coords and "longitude" not in ds.coords:
        rename_map["lon"] = "longitude"
    if rename_map:
        ds = ds.rename(rename_map)
    return ds


def _slice_bbox(ds: xr.Dataset, bbox: TurkeyBBox) -> xr.Dataset:
    if "latitude" not in ds.coords or "longitude" not in ds.coords:
        raise RuntimeError("CAMS dataset missing latitude/longitude coordinates")

    lat_vals = ds["latitude"].values
    lon_vals = ds["longitude"].values

    lat_slice = slice(bbox.north, bbox.south)
    if len(lat_vals) >= 2 and float(lat_vals[0]) < float(lat_vals[-1]):
        lat_slice = slice(bbox.south, bbox.north)

    lon_slice = slice(bbox.west, bbox.east)
    if len(lon_vals) >= 2 and float(lon_vals[0]) > float(lon_vals[-1]):
        lon_slice = slice(bbox.east, bbox.west)

    return ds.sel(latitude=lat_slice, longitude=lon_slice)


def _pick_var(ds: xr.Dataset, aliases: tuple[str, ...]) -> str:
    for alias in aliases:
        if alias in ds.data_vars:
            return alias
        upper = alias.upper()
        if upper in ds.data_vars:
            return upper
    for name in ds.data_vars:
        lowered = name.lower()
        if lowered in aliases:
            return name
    raise RuntimeError(f"CAMS variable not found. tried={aliases}, available={sorted(ds.data_vars)}")


def _normalize_units_to_ugm3(da: xr.DataArray) -> tuple[xr.DataArray, str]:
    units_raw = str(da.attrs.get("units", "")).strip()
    normalized = units_raw.replace("µ", "u").replace("μ", "u").lower().replace(" ", "")

    is_per_m3 = bool(re.search(r"/m\^?3", normalized)) or "m-3" in normalized
    if "kg" in normalized and is_per_m3:
        out = da * 1e9
        out.attrs.update(da.attrs)
        out.attrs["units"] = "ug m-3"
        return out, "kg/m3_to_ugm3_x1e9"

    if "ug" in normalized and ("/m3" in normalized or "m-3" in normalized or "m^3" in normalized):
        return da, "already_ugm3"

    return da, f"assumed_ugm3_from_units={units_raw or 'missing'}"


def _open_dataset(path: Path) -> xr.Dataset:
    errors: list[str] = []
    for engine in ("netcdf4", "h5netcdf", "scipy"):
        try:
            return xr.open_dataset(path, engine=engine)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{engine}:{exc}")
    raise RuntimeError(f"Unable to open CAMS NetCDF ({'; '.join(errors)})")


def extract_daily_grid(netcdf_path: Path, bbox: TurkeyBBox | None = None) -> tuple[xr.Dataset, dict[str, str]]:
    ds = _open_dataset(netcdf_path)
    ds = _normalize_coord_names(ds)
    ds = _slice_bbox(ds, bbox or TurkeyBBox())

    if "time" not in ds.coords and "time" not in ds.dims:
        raise RuntimeError(f"CAMS dataset missing time coordinate. dims={dict(ds.dims)}")

    pm25_var = _pick_var(ds, PM25_ALIASES)
    no2_var = _pick_var(ds, NO2_ALIASES)
    o3_var = _pick_var(ds, O3_ALIASES)

    pm25, pm25_rule = _normalize_units_to_ugm3(ds[pm25_var])
    no2, no2_rule = _normalize_units_to_ugm3(ds[no2_var])
    o3, o3_rule = _normalize_units_to_ugm3(ds[o3_var])

    out = xr.Dataset(
        {
            "pm25_mean_ugm3": pm25.resample(time="1D").mean(),
            "no2_mean_ugm3": no2.resample(time="1D").mean(),
            "o3_mean_ugm3": o3.resample(time="1D").mean(),
        }
    )

    unit_notes = {
        "pm25_mean_ugm3": pm25_rule,
        "no2_mean_ugm3": no2_rule,
        "o3_mean_ugm3": o3_rule,
    }
    return out, unit_notes
