from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import requests

from app.config import settings

try:
    from sentinelhub import BBox, CRS, DataCollection, MimeType, MosaickingOrder, SHConfig, SentinelHubRequest
except Exception:  # noqa: BLE001
    BBox = CRS = DataCollection = MimeType = MosaickingOrder = SHConfig = SentinelHubRequest = None  # type: ignore[assignment]

SENTINEL_EVALSCRIPT = """
//VERSION=3
function setup() {
  return {
    input: ["B04", "B08", "B11", "dataMask"],
    output: { bands: 4, sampleType: "FLOAT32" }
  };
}
function evaluatePixel(s) {
  let ndvi = (s.B08 - s.B04) / (s.B08 + s.B04);
  let nbr  = (s.B08 - s.B11) / (s.B08 + s.B11);
  let bai  = 1.0 / (Math.pow(0.1 - s.B04, 2) + Math.pow(0.06 - s.B08, 2));
  return [ndvi, nbr, bai, s.dataMask];
}
"""


def validate_sentinel_runtime() -> list[str]:
    missing: list[str] = []
    if not settings.era5_gcs_bucket:
        missing.append("ERA5_GCS_BUCKET")
    if not settings.sentinel_hub_client_id:
        missing.append("SENTINEL_HUB_CLIENT_ID")
    if not settings.sentinel_hub_client_secret:
        missing.append("SENTINEL_HUB_CLIENT_SECRET")
    if SHConfig is None:
        missing.append("sentinelhub package")
    return missing


def request_access_token() -> str:
    if not settings.sentinel_hub_client_id or not settings.sentinel_hub_client_secret:
        raise RuntimeError("Sentinel Hub credentials are missing")

    response = requests.post(
        settings.sentinel_hub_token_url,
        data={"grant_type": "client_credentials"},
        auth=(settings.sentinel_hub_client_id, settings.sentinel_hub_client_secret),
        timeout=20,
    )

    if response.status_code in {401, 403}:
        raise RuntimeError("Sentinel Hub authorization failed with 401/403")
    if response.status_code >= 400:
        raise RuntimeError(f"Sentinel Hub token request failed with HTTP {response.status_code}")

    payload: dict[str, Any] = response.json()
    token = str(payload.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Sentinel Hub token response did not include access_token")
    return token


def _build_config() -> Any:
    if SHConfig is None:
        raise RuntimeError("sentinelhub package is not installed")
    config = SHConfig()
    config.sh_client_id = settings.sentinel_hub_client_id
    config.sh_client_secret = settings.sentinel_hub_client_secret
    return config


def fetch_monthly_raster(
    *,
    bbox: tuple[float, float, float, float],
    month_start: date,
    month_end: date,
) -> np.ndarray:
    if any(x is None for x in (BBox, CRS, DataCollection, MimeType, MosaickingOrder, SentinelHubRequest)):
        raise RuntimeError("sentinelhub package is not installed")
    req = SentinelHubRequest(
        evalscript=SENTINEL_EVALSCRIPT,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A,
                time_interval=(month_start.isoformat(), month_end.isoformat()),
                mosaicking_order=MosaickingOrder.LEAST_CC,
            )
        ],
        responses=[SentinelHubRequest.output_response("default", MimeType.TIFF)],
        bbox=BBox(bbox=bbox, crs=CRS.WGS84),
        resolution=(int(settings.sentinel_resolution_m), int(settings.sentinel_resolution_m)),
        config=_build_config(),
    )
    data = req.get_data(save_data=False)
    if not data:
        raise RuntimeError("Sentinel Hub returned no raster data")
    arr = np.asarray(data[0], dtype=np.float32)
    if arr.size == 0:
        raise RuntimeError("Sentinel Hub returned an empty raster")
    return arr
