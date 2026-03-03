from __future__ import annotations

from datetime import date

import pandas as pd

from pipeline.openmeteo_pipeline import compute_spi_proxy_30d, kmh_to_ms, merge_partition_frames, mj_per_day_to_wm2


def test_openmeteo_unit_conversions():
    wind = pd.Series([0.0, 3.6, 36.0])
    solar = pd.Series([0.0, 8.64, 17.28])

    wind_ms = kmh_to_ms(wind)
    solar_wm2 = mj_per_day_to_wm2(solar)

    assert round(float(wind_ms.iloc[1]), 3) == 1.0
    assert round(float(wind_ms.iloc[2]), 3) == 10.0
    assert round(float(solar_wm2.iloc[1]), 3) == 100.0
    assert round(float(solar_wm2.iloc[2]), 3) == 200.0


def test_spi_proxy_handles_std_zero_as_null():
    rows = []
    for i in range(40):
        rows.append({"point_id": "34", "date": date(2024, 1, 1) + pd.Timedelta(days=i), "precip_sum_mm": 10.0})
    df = pd.DataFrame(rows)

    out = compute_spi_proxy_30d(df)

    assert out["spi_proxy_30d"].isna().all()


def test_spi_proxy_computes_after_30_days():
    rows = []
    for i in range(40):
        rows.append({"point_id": "06", "date": date(2024, 1, 1) + pd.Timedelta(days=i), "precip_sum_mm": float(i)})
    df = pd.DataFrame(rows)

    out = compute_spi_proxy_30d(df)

    assert out.loc[out.index[0:29], "spi_proxy_30d"].isna().all()
    assert out.loc[out.index[39], "spi_proxy_30d"] == out.loc[out.index[39], "spi_proxy_30d"]


def test_month_partition_merge_keeps_latest_duplicate():
    existing = pd.DataFrame(
        [
            {"date": date(2026, 2, 1), "point_id": "34", "precip_sum_mm": 5.0},
            {"date": date(2026, 2, 2), "point_id": "34", "precip_sum_mm": 6.0},
        ]
    )
    new = pd.DataFrame(
        [
            {"date": date(2026, 2, 2), "point_id": "34", "precip_sum_mm": 8.0},
            {"date": date(2026, 2, 3), "point_id": "34", "precip_sum_mm": 9.0},
        ]
    )

    merged = merge_partition_frames(existing, new, dedupe_keys=["date", "point_id"])

    assert len(merged) == 3
    row = merged[merged["date"] == date(2026, 2, 2)].iloc[0]
    assert float(row["precip_sum_mm"]) == 8.0
