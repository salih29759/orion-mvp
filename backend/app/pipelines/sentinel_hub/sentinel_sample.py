from __future__ import annotations

import pandas as pd
from sqlalchemy import select

from app.database import SessionLocal
from app.orm import ProvinceORM

TURKEY_BOUNDS = {
    "north": 42.0,
    "west": 26.0,
    "south": 36.0,
    "east": 45.0,
}


def load_province_centroids() -> pd.DataFrame:
    with SessionLocal() as db:
        rows = db.execute(select(ProvinceORM.plate, ProvinceORM.lat, ProvinceORM.lng).order_by(ProvinceORM.plate)).all()
    if not rows:
        raise RuntimeError("No provinces found in database")

    frame = pd.DataFrame(
        [
            {
                "province_id": f"province:{int(plate):02d}",
                "lat": float(lat),
                "lon": float(lon),
            }
            for plate, lat, lon in rows
        ]
    )
    return frame


def centroid_bbox(*, lat: float, lon: float, half_size_deg: float) -> tuple[float, float, float, float]:
    lat_min = max(TURKEY_BOUNDS["south"], float(lat) - float(half_size_deg))
    lat_max = min(TURKEY_BOUNDS["north"], float(lat) + float(half_size_deg))
    lon_min = max(TURKEY_BOUNDS["west"], float(lon) - float(half_size_deg))
    lon_max = min(TURKEY_BOUNDS["east"], float(lon) + float(half_size_deg))
    return lon_min, lat_min, lon_max, lat_max
