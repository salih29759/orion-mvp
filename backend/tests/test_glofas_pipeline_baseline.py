from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import xarray as xr

from pipeline import glofas_pipeline


def _synthetic_dataset(days: int = 3) -> xr.Dataset:
    times = pd.date_range("2020-01-01", periods=days, freq="D")
    lat = np.array([41.0, 40.0])
    lon = np.array([29.0, 30.0])

    data = np.zeros((days, 2, 2), dtype=float)
    data[:, 0, 0] = np.array([10.0, 20.0, 30.0])[:days]
    data[:, 1, 1] = np.array([5.0, 15.0, 25.0])[:days]

    return xr.Dataset(
        {"dis24": (("time", "latitude", "longitude"), data)},
        coords={"time": times, "latitude": lat, "longitude": lon},
    )


def test_point_mapping_and_month_frame_row_count():
    ds = _synthetic_dataset(days=3)
    points = [
        glofas_pipeline.ProvincePoint(point_id="province:01", lat=41.02, lon=28.98),
        glofas_pipeline.ProvincePoint(point_id="province:02", lat=39.98, lon=30.02),
    ]

    mapping = glofas_pipeline._build_point_mapping(ds, "dis24", points)
    assert mapping["province:01"] == (41.0, 29.0)
    assert mapping["province:02"] == (40.0, 30.0)

    frame = glofas_pipeline._build_month_frame(
        ds,
        var_name="dis24",
        points=points,
        point_to_grid=mapping,
        run_id="run-1",
        baseline_lookup={},
        ingested_at=datetime.now(timezone.utc),
    )

    assert len(frame) == 6
    assert set(frame.columns) == {
        "date",
        "point_id",
        "lat",
        "lon",
        "river_discharge_m3s",
        "discharge_anomaly_pct",
        "flood_flag",
        "source",
        "run_id",
        "ingested_at",
    }
    assert frame["flood_flag"].eq(False).all()


def test_baseline_calculation_and_anomaly_flagging():
    hist = pd.DataFrame(
        [
            {"date": "2020-01-01", "point_id": "province:01", "river_discharge_m3s": 10.0},
            {"date": "2021-01-01", "point_id": "province:01", "river_discharge_m3s": 30.0},
            {"date": "2020-01-02", "point_id": "province:01", "river_discharge_m3s": 20.0},
            {"date": "2021-01-02", "point_id": "province:01", "river_discharge_m3s": 40.0},
        ]
    )
    baseline = glofas_pipeline._calculate_baseline_frame(hist)
    lookup = glofas_pipeline._build_baseline_lookup(baseline)

    enriched = glofas_pipeline._enrich_with_baseline(
        pd.DataFrame(
            [
                {
                    "date": "2022-01-01",
                    "point_id": "province:01",
                    "lat": 41.0,
                    "lon": 29.0,
                    "river_discharge_m3s": 40.0,
                }
            ]
        ),
        lookup,
    )

    anomaly = float(enriched.iloc[0]["discharge_anomaly_pct"])
    assert round(anomaly, 2) == 100.0
    assert bool(enriched.iloc[0]["flood_flag"]) is True
