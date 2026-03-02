from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import tempfile
import time

import cdsapi

from app.config import settings


def cds_is_configured() -> bool:
    return bool(settings.cdsapi_key)


def _target_file() -> Path:
    ts = int(time.time())
    return Path(tempfile.gettempdir()) / f"orion_cds_test_{ts}.nc"


def _base_request(sample_date: date) -> dict:
    return {
        "product_type": "reanalysis",
        "variable": [settings.cds_variable],
        "year": [sample_date.strftime("%Y")],
        "month": [sample_date.strftime("%m")],
        "day": [sample_date.strftime("%d")],
        "time": ["00:00"],
        "area": [
            settings.cds_area_north,
            settings.cds_area_west,
            settings.cds_area_south,
            settings.cds_area_east,
        ],
    }


def run_cds_smoke_test() -> dict:
    if not cds_is_configured():
        raise RuntimeError("CDSAPI_KEY is missing")

    sample_date = date.today() - timedelta(days=7)
    target = _target_file()

    client = cdsapi.Client(url=settings.cdsapi_url, key=settings.cdsapi_key, quiet=True)
    request = _base_request(sample_date)

    # Try legacy format first, then new data_format/download_format style.
    attempts = [
        {**request, "format": "netcdf"},
        {**request, "data_format": "netcdf", "download_format": "unarchived"},
    ]

    last_error = None
    for payload in attempts:
        try:
            client.retrieve(settings.cds_dataset, payload, str(target))
            size = target.stat().st_size if target.exists() else 0
            return {
                "status": "success",
                "dataset": settings.cds_dataset,
                "variable": settings.cds_variable,
                "sample_date": sample_date.isoformat(),
                "target_file": str(target),
                "bytes": size,
            }
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

    raise RuntimeError(f"CDS smoke test failed: {last_error}")
