from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from math import cos, floor, radians
from pathlib import Path
from typing import Any
from uuid import uuid4
import json
import tempfile
import threading

import numpy as np
import pandas as pd
import requests
from sqlalchemy import desc, select

from app.config import settings
from app.database import SessionLocal
from app.orm import DemJobRunORM, ProvinceORM

TURKEY_NORTH = 42.0
TURKEY_WEST = 26.0
TURKEY_SOUTH = 36.0
TURKEY_EAST = 45.0
BUFFER_KM = 10.0
GRID_STEP_DEG = 0.1
GRID_HALF_CELL_DEG = 0.05

SOURCE_GLO30 = "copernicus_dem_glo30"
SOURCE_GLO90 = "copernicus_dem_glo90"

TILE_LIST_URL_30 = "https://copernicus-dem-30m.s3.amazonaws.com/tileList.txt"
TILE_LIST_URL_90 = "https://copernicus-dem-90m.s3.amazonaws.com/tileList.txt"

BUCKET_URL_30 = "https://copernicus-dem-30m.s3.amazonaws.com"
BUCKET_URL_90 = "https://copernicus-dem-90m.s3.amazonaws.com"

PROVINCE_OBJECT = "reference/dem/province_elevation_stats.parquet"
GRID_OBJECT = "reference/dem/turkey_elevation_grid_0p1deg.parquet"

STATUS_IDLE = "idle"
STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_SUCCESS_WITH_WARNINGS = "success_with_warnings"


@dataclass(frozen=True)
class TileRef:
    source: str
    tile_id: str
    lat_idx: int
    lon_idx: int
    url: str


def _import_rasterio():
    try:
        import rasterio  # type: ignore
        from rasterio.windows import Window, from_bounds  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("rasterio is required for DEM processing") from exc
    return rasterio, Window, from_bounds


def validate_dem_runtime() -> list[str]:
    missing: list[str] = []
    if not settings.era5_gcs_bucket:
        missing.append("ERA5_GCS_BUCKET")
    try:
        _import_rasterio()
    except RuntimeError:
        missing.append("rasterio")
    return missing


def _default_progress() -> dict[str, int]:
    return {
        "tiles_total": 0,
        "tiles_glo30": 0,
        "tiles_glo90": 0,
        "provinces_total": 0,
        "provinces_done": 0,
        "grid_cells_total": 0,
        "grid_cells_done": 0,
        "warning_count": 0,
    }


def _parse_progress(raw: str | None) -> dict[str, int]:
    base = _default_progress()
    if not raw:
        return base
    try:
        parsed = json.loads(raw)
    except Exception:  # noqa: BLE001
        return base
    if isinstance(parsed, dict):
        for key in base:
            value = parsed.get(key)
            if isinstance(value, (int, float)):
                base[key] = int(value)
    return base


def _format_tile_id(*, arcsec: int, lat_idx: int, lon_idx: int) -> str:
    ns = "N" if lat_idx >= 0 else "S"
    ew = "E" if lon_idx >= 0 else "W"
    lat_abs = abs(int(lat_idx))
    lon_abs = abs(int(lon_idx))
    return f"Copernicus_DSM_COG_{arcsec}_{ns}{lat_abs:02d}_00_{ew}{lon_abs:03d}_00_DEM"


def _download_text(url: str) -> str:
    res = requests.get(url, timeout=60)
    res.raise_for_status()
    return res.text.replace("\r", "")


def _tile_list_cache_path(source: str) -> Path:
    suffix = "30" if source == SOURCE_GLO30 else "90"
    return Path(tempfile.gettempdir()) / f"orion_dem_tilelist_{suffix}.txt"


def _load_tile_list(source: str) -> set[str]:
    cache_path = _tile_list_cache_path(source)
    if cache_path.exists():
        text = cache_path.read_text(encoding="utf-8")
    else:
        text = _download_text(TILE_LIST_URL_30 if source == SOURCE_GLO30 else TILE_LIST_URL_90)
        cache_path.write_text(text, encoding="utf-8")
    return {line.strip() for line in text.splitlines() if line.strip()}


def _haversine_km(lat1, lon1, lat2, lon2):
    lat1r = np.radians(lat1)
    lon1r = np.radians(lon1)
    lat2r = np.radians(lat2)
    lon2r = np.radians(lon2)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1r) * np.cos(lat2r) * (np.sin(dlon / 2.0) ** 2)
    return 2.0 * 6371.0 * np.arcsin(np.sqrt(a))


class TileManager:
    def __init__(self) -> None:
        self.tiles_30 = _load_tile_list(SOURCE_GLO30)
        self.tiles_90 = _load_tile_list(SOURCE_GLO90)
        self._resolved: dict[tuple[int, int], TileRef | None] = {}
        self._download_cache: dict[str, Path] = {}
        self._datasets: OrderedDict[str, Any] = OrderedDict()
        self._datasets_lock = threading.Lock()
        self.max_open = 16
        self.cache_dir = Path(tempfile.gettempdir()) / "orion_dem_tiles"

    def resolve(self, lat_idx: int, lon_idx: int) -> TileRef | None:
        key = (int(lat_idx), int(lon_idx))
        if key in self._resolved:
            return self._resolved[key]

        tile30 = _format_tile_id(arcsec=10, lat_idx=key[0], lon_idx=key[1])
        if tile30 in self.tiles_30:
            tile = TileRef(
                source=SOURCE_GLO30,
                tile_id=tile30,
                lat_idx=key[0],
                lon_idx=key[1],
                url=f"{BUCKET_URL_30}/{tile30}/{tile30}.tif",
            )
            self._resolved[key] = tile
            return tile

        tile90 = _format_tile_id(arcsec=30, lat_idx=key[0], lon_idx=key[1])
        if tile90 in self.tiles_90:
            tile = TileRef(
                source=SOURCE_GLO90,
                tile_id=tile90,
                lat_idx=key[0],
                lon_idx=key[1],
                url=f"{BUCKET_URL_90}/{tile90}/{tile90}.tif",
            )
            self._resolved[key] = tile
            return tile

        self._resolved[key] = None
        return None

    def resolve_point(self, *, lat: float, lon: float) -> TileRef | None:
        return self.resolve(floor(lat), floor(lon))

    def resolve_bounds(self, *, north: float, west: float, south: float, east: float) -> list[TileRef]:
        lat_min = floor(south)
        lat_max = floor(north)
        lon_min = floor(west)
        lon_max = floor(east)
        tiles: list[TileRef] = []
        for lat_idx in range(lat_min, lat_max + 1):
            for lon_idx in range(lon_min, lon_max + 1):
                tile = self.resolve(lat_idx, lon_idx)
                if tile is not None:
                    tiles.append(tile)
        return tiles

    def _tile_path(self, tile: TileRef) -> Path:
        source_dir = "glo30" if tile.source == SOURCE_GLO30 else "glo90"
        return self.cache_dir / source_dir / f"{tile.tile_id}.tif"

    def download(self, tile: TileRef) -> Path:
        if tile.tile_id in self._download_cache and self._download_cache[tile.tile_id].exists():
            return self._download_cache[tile.tile_id]

        path = self._tile_path(tile)
        if path.exists():
            self._download_cache[tile.tile_id] = path
            return path

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{uuid4().hex}.tmp")
        res = requests.get(tile.url, stream=True, timeout=(30, 600))
        res.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in res.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
        tmp.rename(path)
        self._download_cache[tile.tile_id] = path
        return path

    def prefetch(self, tiles: list[TileRef], max_workers: int = 4) -> None:
        dedup = {tile.tile_id: tile for tile in tiles}
        if not dedup:
            return
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(self.download, tile) for tile in dedup.values()]
            for future in as_completed(futures):
                future.result()

    def open_dataset(self, tile: TileRef):
        rasterio, _Window, _from_bounds = _import_rasterio()
        key = tile.tile_id
        with self._datasets_lock:
            if key in self._datasets:
                ds = self._datasets.pop(key)
                self._datasets[key] = ds
                return ds

            path = self.download(tile)
            ds = rasterio.open(path)
            self._datasets[key] = ds
            while len(self._datasets) > self.max_open:
                _old_key, old_ds = self._datasets.popitem(last=False)
                old_ds.close()
            return ds

    def close(self) -> None:
        with self._datasets_lock:
            for ds in self._datasets.values():
                ds.close()
            self._datasets.clear()


def _intersect_bounds(ds, *, north: float, west: float, south: float, east: float) -> tuple[float, float, float, float] | None:
    left = float(ds.bounds.left)
    right = float(ds.bounds.right)
    bottom = float(ds.bounds.bottom)
    top = float(ds.bounds.top)

    i_west = max(west, left)
    i_east = min(east, right)
    i_south = max(south, bottom)
    i_north = min(north, top)

    if i_west >= i_east or i_south >= i_north:
        return None
    return (i_north, i_west, i_south, i_east)


def _read_window(ds, *, north: float, west: float, south: float, east: float):
    rasterio, Window, from_bounds = _import_rasterio()
    window = from_bounds(west, south, east, north, transform=ds.transform)
    full_window = Window(col_off=0, row_off=0, width=ds.width, height=ds.height)
    window = window.round_offsets().round_lengths().intersection(full_window)
    if window.width <= 0 or window.height <= 0:
        return None

    arr = ds.read(1, window=window, masked=True)
    if arr.size == 0:
        return None

    elev = np.asarray(arr.filled(np.nan), dtype="float64")
    mask = np.ma.getmaskarray(arr)
    if mask is not False:
        elev[mask] = np.nan

    row_start = int(window.row_off)
    row_end = row_start + int(window.height)
    col_start = int(window.col_off)
    col_end = col_start + int(window.width)

    rows = np.arange(row_start, row_end, dtype="float64") + 0.5
    cols = np.arange(col_start, col_end, dtype="float64") + 0.5
    col_grid, row_grid = np.meshgrid(cols, rows)

    transform = ds.transform
    lons = transform.c + (col_grid * transform.a) + (row_grid * transform.b)
    lats = transform.f + (col_grid * transform.d) + (row_grid * transform.e)

    lat_ref = float(np.nanmean(lats)) if np.isfinite(np.nanmean(lats)) else 39.0
    dy_m = max(abs(float(transform.e)) * 111_320.0, 1e-6)
    dx_m = max(abs(float(transform.a)) * 111_320.0 * max(cos(radians(lat_ref)), 1e-6), 1e-6)

    if elev.shape[0] < 2 or elev.shape[1] < 2:
        slope = np.full_like(elev, np.nan, dtype="float64")
    else:
        grad_y, grad_x = np.gradient(elev, dy_m, dx_m)
        slope = np.degrees(np.arctan(np.sqrt((grad_x ** 2) + (grad_y ** 2))))
        slope[~np.isfinite(elev)] = np.nan

    return lats, lons, elev, slope


def _aggregate_metrics(elev_values: list[np.ndarray], slope_values: list[np.ndarray]) -> tuple[float | None, float | None, float | None, float | None, float | None]:
    if not elev_values:
        return None, None, None, None, None

    elev = np.concatenate([v for v in elev_values if v.size > 0], axis=0) if elev_values else np.array([], dtype="float64")
    slope = np.concatenate([v for v in slope_values if v.size > 0], axis=0) if slope_values else np.array([], dtype="float64")

    elev = elev[np.isfinite(elev)]
    slope = slope[np.isfinite(slope)]

    if elev.size == 0:
        return None, None, None, None, None

    mean_elev = float(np.nanmean(elev))
    min_elev = float(np.nanmin(elev))
    max_elev = float(np.nanmax(elev))
    mean_slope = float(np.nanmean(slope)) if slope.size else None
    max_slope = float(np.nanmax(slope)) if slope.size else None
    return mean_elev, min_elev, max_elev, mean_slope, max_slope


def _sample_point(tile_manager: TileManager, *, lat: float, lon: float) -> tuple[float | None, str | None, bool]:
    tile = tile_manager.resolve_point(lat=lat, lon=lon)
    if tile is None:
        return None, None, True

    ds = tile_manager.open_dataset(tile)
    sampled = list(ds.sample([(lon, lat)]))
    if not sampled:
        return None, tile.source, True

    value = float(sampled[0][0])
    if ds.nodata is not None and abs(value - float(ds.nodata)) < 1e-9:
        return None, tile.source, True
    if not np.isfinite(value):
        return None, tile.source, True
    return value, tile.source, False


def _buffer_bounds(*, lat: float, lon: float, km: float) -> tuple[float, float, float, float]:
    lat_delta = km / 111.0
    cos_lat = max(cos(radians(lat)), 1e-6)
    lon_delta = km / (111.0 * cos_lat)
    return lat + lat_delta, lon - lon_delta, lat - lat_delta, lon + lon_delta


def _buffer_stats(tile_manager: TileManager, *, lat: float, lon: float, buffer_km: float) -> tuple[float | None, float | None, float | None, float | None, float | None, bool]:
    north, west, south, east = _buffer_bounds(lat=lat, lon=lon, km=buffer_km)
    tiles = tile_manager.resolve_bounds(north=north, west=west, south=south, east=east)
    if not tiles:
        return None, None, None, None, None, True

    elev_values: list[np.ndarray] = []
    slope_values: list[np.ndarray] = []

    for tile in tiles:
        ds = tile_manager.open_dataset(tile)
        intersection = _intersect_bounds(ds, north=north, west=west, south=south, east=east)
        if intersection is None:
            continue
        window = _read_window(
            ds,
            north=intersection[0],
            west=intersection[1],
            south=intersection[2],
            east=intersection[3],
        )
        if window is None:
            continue

        lats, lons, elev, slope = window
        dist = _haversine_km(lats, lons, lat, lon)
        inside = dist <= buffer_km
        valid = inside & np.isfinite(elev)
        if np.any(valid):
            elev_values.append(elev[valid])
            slope_valid = valid & np.isfinite(slope)
            if np.any(slope_valid):
                slope_values.append(slope[slope_valid])

    mean_elev, min_elev, max_elev, mean_slope, max_slope = _aggregate_metrics(elev_values, slope_values)
    warn = mean_elev is None
    return mean_elev, min_elev, max_elev, mean_slope, max_slope, warn


def _rect_stats(tile_manager: TileManager, *, north: float, west: float, south: float, east: float) -> tuple[float | None, float | None, float | None, float | None, float | None, bool]:
    tiles = tile_manager.resolve_bounds(north=north, west=west, south=south, east=east)
    if not tiles:
        return None, None, None, None, None, True

    elev_values: list[np.ndarray] = []
    slope_values: list[np.ndarray] = []

    for tile in tiles:
        ds = tile_manager.open_dataset(tile)
        intersection = _intersect_bounds(ds, north=north, west=west, south=south, east=east)
        if intersection is None:
            continue
        window = _read_window(
            ds,
            north=intersection[0],
            west=intersection[1],
            south=intersection[2],
            east=intersection[3],
        )
        if window is None:
            continue

        _lats, _lons, elev, slope = window
        valid = np.isfinite(elev)
        if np.any(valid):
            elev_values.append(elev[valid])
        slope_valid = np.isfinite(slope)
        if np.any(slope_valid):
            slope_values.append(slope[slope_valid])

    mean_elev, min_elev, max_elev, mean_slope, max_slope = _aggregate_metrics(elev_values, slope_values)
    warn = mean_elev is None
    return mean_elev, min_elev, max_elev, mean_slope, max_slope, warn


def _upload_parquet(frame: pd.DataFrame, object_name: str) -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")

    from google.cloud import storage

    local = Path(tempfile.gettempdir()) / f"orion_dem_{uuid4().hex}.parquet"
    frame.to_parquet(local, index=False)
    try:
        client = storage.Client()
        bucket = client.bucket(settings.era5_gcs_bucket)
        bucket.blob(object_name).upload_from_filename(str(local))
    finally:
        try:
            local.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass

    return f"gs://{settings.era5_gcs_bucket}/{object_name}"


def _grid_centers() -> list[tuple[float, float]]:
    lat_count = int(round((TURKEY_NORTH - TURKEY_SOUTH) / GRID_STEP_DEG))
    lon_count = int(round((TURKEY_EAST - TURKEY_WEST) / GRID_STEP_DEG))
    out: list[tuple[float, float]] = []
    for i in range(lat_count):
        lat = TURKEY_SOUTH + GRID_HALF_CELL_DEG + (i * GRID_STEP_DEG)
        for j in range(lon_count):
            lon = TURKEY_WEST + GRID_HALF_CELL_DEG + (j * GRID_STEP_DEG)
            out.append((round(lat, 4), round(lon, 4)))
    return out


def _nearest_province_id(*, lat: float, lon: float, province_ids: np.ndarray, province_lats: np.ndarray, province_lons: np.ndarray) -> str:
    dist = _haversine_km(province_lats, province_lons, lat, lon)
    idx = int(np.nanargmin(dist))
    return str(province_ids[idx])


def _load_provinces() -> pd.DataFrame:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM.id, ProvinceORM.lat, ProvinceORM.lng).order_by(ProvinceORM.id)).all()
    if not rows:
        raise RuntimeError("No provinces found in database")
    return pd.DataFrame(rows, columns=["province_id", "lat", "lon"])


def _update_run(run_id: str, *, status: str | None = None, progress: dict[str, int] | None = None, province_gcs_uri: str | None = None,
                grid_gcs_uri: str | None = None, error: str | None = None, started_at: datetime | None = None,
                finished_at: datetime | None = None) -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        row = db.get(DemJobRunORM, run_id)
        if row is None:
            return
        if status is not None:
            row.status = status
        if progress is not None:
            row.progress_json = json.dumps(progress)
        if province_gcs_uri is not None:
            row.province_gcs_uri = province_gcs_uri
        if grid_gcs_uri is not None:
            row.grid_gcs_uri = grid_gcs_uri
        if error is not None:
            row.error = error[:4000]
        if started_at is not None:
            row.started_at = started_at
        if finished_at is not None:
            row.finished_at = finished_at
        row.updated_at = now
        db.commit()


def _collect_required_tiles(tile_manager: TileManager, provinces: pd.DataFrame, include_grid: bool) -> list[TileRef]:
    tiles: dict[str, TileRef] = {}

    for province in provinces.itertuples(index=False):
        tile = tile_manager.resolve_point(lat=float(province.lat), lon=float(province.lon))
        if tile is not None:
            tiles[tile.tile_id] = tile
        north, west, south, east = _buffer_bounds(lat=float(province.lat), lon=float(province.lon), km=BUFFER_KM)
        for t in tile_manager.resolve_bounds(north=north, west=west, south=south, east=east):
            tiles[t.tile_id] = t

    if include_grid:
        for t in tile_manager.resolve_bounds(
            north=TURKEY_NORTH,
            west=TURKEY_WEST,
            south=TURKEY_SOUTH,
            east=TURKEY_EAST,
        ):
            tiles[t.tile_id] = t

    return list(tiles.values())


def _run_dem_job(*, run_id: str, include_grid: bool) -> None:
    started = datetime.now(timezone.utc)
    progress = _default_progress()
    tile_manager = TileManager()
    warning_count = 0

    try:
        _update_run(run_id, status=STATUS_RUNNING, started_at=started, progress=progress)
        created_at = datetime.now(timezone.utc)

        provinces = _load_provinces()
        province_ids = provinces["province_id"].to_numpy()
        province_lats = provinces["lat"].to_numpy(dtype="float64")
        province_lons = provinces["lon"].to_numpy(dtype="float64")

        required_tiles = _collect_required_tiles(tile_manager, provinces, include_grid)
        tile_manager.prefetch(required_tiles, max_workers=4)

        progress["tiles_total"] = len(required_tiles)
        progress["tiles_glo30"] = sum(1 for t in required_tiles if t.source == SOURCE_GLO30)
        progress["tiles_glo90"] = sum(1 for t in required_tiles if t.source == SOURCE_GLO90)

        progress["provinces_total"] = int(len(provinces.index))
        _update_run(run_id, progress=progress)

        province_rows: list[dict[str, Any]] = []
        for idx, province in enumerate(provinces.itertuples(index=False), start=1):
            lat = float(province.lat)
            lon = float(province.lon)

            elevation_m, source, point_warn = _sample_point(tile_manager, lat=lat, lon=lon)
            (
                elevation_mean_m,
                elevation_min_m,
                elevation_max_m,
                slope_mean_deg,
                slope_max_deg,
                buffer_warn,
            ) = _buffer_stats(tile_manager, lat=lat, lon=lon, buffer_km=BUFFER_KM)

            if point_warn:
                warning_count += 1
            if buffer_warn:
                warning_count += 1

            province_rows.append(
                {
                    "province_id": str(province.province_id),
                    "lat": lat,
                    "lon": lon,
                    "elevation_m": elevation_m,
                    "elevation_mean_m": elevation_mean_m,
                    "elevation_min_m": elevation_min_m,
                    "elevation_max_m": elevation_max_m,
                    "slope_mean_deg": slope_mean_deg,
                    "slope_max_deg": slope_max_deg,
                    "source": source or SOURCE_GLO90,
                    "created_at": created_at,
                }
            )

            progress["provinces_done"] = idx
            progress["warning_count"] = warning_count
            if idx % 5 == 0 or idx == progress["provinces_total"]:
                _update_run(run_id, progress=progress)

        province_frame = pd.DataFrame(province_rows)
        province_uri = _upload_parquet(province_frame, PROVINCE_OBJECT)
        _update_run(run_id, province_gcs_uri=province_uri, progress=progress)

        grid_uri: str | None = None
        if include_grid:
            centers = _grid_centers()
            progress["grid_cells_total"] = len(centers)
            _update_run(run_id, progress=progress)

            grid_rows: list[dict[str, Any]] = []
            for idx, (lat, lon) in enumerate(centers, start=1):
                north = lat + GRID_HALF_CELL_DEG
                west = lon - GRID_HALF_CELL_DEG
                south = lat - GRID_HALF_CELL_DEG
                east = lon + GRID_HALF_CELL_DEG

                (
                    elevation_mean_m,
                    elevation_min_m,
                    elevation_max_m,
                    slope_mean_deg,
                    slope_max_deg,
                    rect_warn,
                ) = _rect_stats(tile_manager, north=north, west=west, south=south, east=east)
                if rect_warn:
                    warning_count += 1

                center_tile = tile_manager.resolve_point(lat=lat, lon=lon)
                source = center_tile.source if center_tile is not None else SOURCE_GLO90
                nearest_pid = _nearest_province_id(
                    lat=lat,
                    lon=lon,
                    province_ids=province_ids,
                    province_lats=province_lats,
                    province_lons=province_lons,
                )
                grid_rows.append(
                    {
                        "grid_id": f"{lat:.2f}_{lon:.2f}",
                        "lat": lat,
                        "lon": lon,
                        "elevation_mean_m": elevation_mean_m,
                        "elevation_min_m": elevation_min_m,
                        "elevation_max_m": elevation_max_m,
                        "slope_mean_deg": slope_mean_deg,
                        "slope_max_deg": slope_max_deg,
                        "province_id": nearest_pid,
                        "source": source,
                        "created_at": created_at,
                    }
                )

                progress["grid_cells_done"] = idx
                progress["warning_count"] = warning_count
                if idx % 100 == 0 or idx == progress["grid_cells_total"]:
                    _update_run(run_id, progress=progress)

            grid_frame = pd.DataFrame(grid_rows)
            grid_uri = _upload_parquet(grid_frame, GRID_OBJECT)
            _update_run(run_id, grid_gcs_uri=grid_uri, progress=progress)

        final_status = STATUS_SUCCESS_WITH_WARNINGS if warning_count > 0 else STATUS_SUCCESS
        progress["warning_count"] = warning_count
        _update_run(
            run_id,
            status=final_status,
            progress=progress,
            province_gcs_uri=province_uri,
            grid_gcs_uri=grid_uri,
            finished_at=datetime.now(timezone.utc),
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        progress["warning_count"] = warning_count
        _update_run(
            run_id,
            status=STATUS_FAILED,
            progress=progress,
            error=str(exc),
            finished_at=datetime.now(timezone.utc),
        )
    finally:
        tile_manager.close()


def create_dem_run(*, include_grid: bool) -> dict:
    run_id = f"dem_{uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)
    progress = _default_progress()

    with SessionLocal() as db:
        db.add(
            DemJobRunORM(
                run_id=run_id,
                status=STATUS_QUEUED,
                include_grid=bool(include_grid),
                progress_json=json.dumps(progress),
                created_at=now,
                updated_at=now,
            )
        )
        db.commit()

    thread = threading.Thread(target=_run_dem_job, kwargs={"run_id": run_id, "include_grid": bool(include_grid)}, daemon=False)
    thread.start()

    return {
        "run_id": run_id,
        "status": STATUS_QUEUED,
        "type": "dem_reference_build",
        "created_at": now,
        "progress": progress,
    }


def get_latest_dem_status() -> dict:
    with SessionLocal() as db:
        row = db.execute(select(DemJobRunORM).order_by(desc(DemJobRunORM.updated_at)).limit(1)).scalar_one_or_none()

    if row is None:
        return {
            "run_id": None,
            "status": STATUS_IDLE,
            "type": "dem_reference_build",
            "include_grid": False,
            "created_at": None,
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "progress": _default_progress(),
            "province_gcs_uri": None,
            "grid_gcs_uri": None,
            "error": None,
        }

    return {
        "run_id": row.run_id,
        "status": row.status,
        "type": "dem_reference_build",
        "include_grid": bool(row.include_grid),
        "created_at": row.created_at,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
        "updated_at": row.updated_at,
        "progress": _parse_progress(row.progress_json),
        "province_gcs_uri": row.province_gcs_uri,
        "grid_gcs_uri": row.grid_gcs_uri,
        "error": row.error,
    }
