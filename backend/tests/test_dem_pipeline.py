from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from pipeline import dem_pipeline

rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin


class _FakeTileManager:
    def __init__(self, tile_path: Path, source: str = dem_pipeline.SOURCE_GLO30):
        self.tile = dem_pipeline.TileRef(source=source, tile_id="fake_tile", lat_idx=39, lon_idx=30, url="file://fake")
        self._path = tile_path
        self._ds = None

    def resolve_point(self, *, lat: float, lon: float):  # noqa: ARG002
        return self.tile

    def resolve_bounds(self, *, north: float, west: float, south: float, east: float):  # noqa: ARG002
        return [self.tile]

    def open_dataset(self, tile):  # noqa: ARG002
        if self._ds is None:
            self._ds = rasterio.open(self._path)
        return self._ds

    def close(self):
        if self._ds is not None:
            self._ds.close()


def _create_sample_tile(path: Path) -> Path:
    data = np.arange(100, dtype="float32").reshape((10, 10))
    transform = from_origin(30.0, 40.0, 0.01, 0.01)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=10,
        width=10,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(data, 1)
    return path


def test_small_tile_province_stats_for_two_points(tmp_path):
    tile_path = _create_sample_tile(tmp_path / "sample_dem.tif")
    manager = _FakeTileManager(tile_path)
    try:
        points = [(39.95, 30.05), (39.97, 30.08)]
        for lat, lon in points:
            elevation_m, source, point_warn = dem_pipeline._sample_point(manager, lat=lat, lon=lon)
            mean_elev, min_elev, max_elev, slope_mean, slope_max, buffer_warn = dem_pipeline._buffer_stats(
                manager,
                lat=lat,
                lon=lon,
                buffer_km=10.0,
            )

            assert point_warn is False
            assert buffer_warn is False
            assert source == dem_pipeline.SOURCE_GLO30
            assert elevation_m is not None
            assert mean_elev is not None
            assert min_elev is not None
            assert max_elev is not None
            assert slope_mean is not None
            assert slope_max is not None
            assert min_elev <= mean_elev <= max_elev
            assert slope_mean >= 0.0
            assert slope_max + 1e-9 >= slope_mean
    finally:
        manager.close()


def test_tile_fallback_uses_glo90_when_30_missing(monkeypatch):
    lat_idx = 39
    lon_idx = 32
    tile30 = dem_pipeline._format_tile_id(arcsec=10, lat_idx=lat_idx, lon_idx=lon_idx)
    tile90 = dem_pipeline._format_tile_id(arcsec=30, lat_idx=lat_idx, lon_idx=lon_idx)

    def _fake_load(source: str) -> set[str]:
        if source == dem_pipeline.SOURCE_GLO30:
            return set()
        return {tile90}

    monkeypatch.setattr(dem_pipeline, "_load_tile_list", _fake_load)
    manager = dem_pipeline.TileManager()
    try:
        resolved = manager.resolve(lat_idx, lon_idx)
        assert resolved is not None
        assert resolved.tile_id != tile30
        assert resolved.tile_id == tile90
        assert resolved.source == dem_pipeline.SOURCE_GLO90
    finally:
        manager.close()
