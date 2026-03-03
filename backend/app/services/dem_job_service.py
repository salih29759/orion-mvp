from __future__ import annotations

from app.errors import ApiError
from pipeline.dem_pipeline import create_dem_run, get_latest_dem_status, validate_dem_runtime


def create_process(*, grid: bool) -> dict:
    missing = validate_dem_runtime()
    if missing:
        raise ApiError(status_code=503, error_code="CONFIG_ERROR", message=f"Missing env vars: {', '.join(missing)}")
    return create_dem_run(include_grid=grid)


def get_status() -> dict:
    return get_latest_dem_status()
