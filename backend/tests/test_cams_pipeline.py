from datetime import date

import numpy as np
import pandas as pd
import xarray as xr

from app.pipelines.cams.cams_client import CamsAvailability, pick_reanalysis_type, resolve_effective_end
from app.pipelines.cams.cams_extract import _normalize_units_to_ugm3
from app.pipelines.cams.cams_job import should_skip_month
from app.pipelines.cams.cams_sample import ProvincePoint, compute_air_quality_index, sample_daily_to_provinces


def test_effective_end_auto_cap():
    availability = CamsAvailability(
        by_type={
            "validated_reanalysis": {date(2024, 3, 1)},
            "interim_reanalysis": {date(2024, 3, 1), date(2024, 4, 1)},
        },
        latest_month=date(2024, 4, 1),
    )

    effective = resolve_effective_end(date(2025, 12, 31), availability)
    assert effective == date(2024, 4, 1)


def test_type_selection_prefers_validated_then_interim():
    availability = CamsAvailability(
        by_type={
            "validated_reanalysis": {date(2024, 1, 1)},
            "interim_reanalysis": {date(2024, 1, 1), date(2024, 2, 1)},
        },
        latest_month=date(2024, 2, 1),
    )

    assert pick_reanalysis_type(date(2024, 1, 15), availability) == "validated_reanalysis"
    assert pick_reanalysis_type(date(2024, 2, 20), availability) == "interim_reanalysis"


def test_aqi_is_arithmetic_mean():
    pm25 = pd.Series([30.0, 60.0])
    no2 = pd.Series([15.0, 30.0])
    o3 = pd.Series([45.0, 30.0])

    out = compute_air_quality_index(pm25, no2, o3)
    assert out.tolist() == [30.0, 40.0]


def test_unit_conversion_kgm3_to_ugm3():
    da = xr.DataArray(np.array([1e-9, 2e-9]), dims=["time"], attrs={"units": "kg m-3"})
    converted, rule = _normalize_units_to_ugm3(da)

    assert rule == "kg/m3_to_ugm3_x1e9"
    assert np.allclose(converted.values, np.array([1.0, 2.0]))


def test_idempotency_skip_when_exists_unless_force():
    assert should_skip_month(month_exists=True, force=False) is True
    assert should_skip_month(month_exists=True, force=True) is False
    assert should_skip_month(month_exists=False, force=False) is False


def test_sampling_row_count_for_30_days_81_provinces():
    times = pd.date_range("2019-01-01", periods=30, freq="D", tz="UTC")
    grid = xr.Dataset(
        {
            "pm25_mean_ugm3": (("time", "latitude", "longitude"), np.full((30, 1, 1), 20.0)),
            "no2_mean_ugm3": (("time", "latitude", "longitude"), np.full((30, 1, 1), 30.0)),
            "o3_mean_ugm3": (("time", "latitude", "longitude"), np.full((30, 1, 1), 40.0)),
        },
        coords={"time": times, "latitude": [39.0], "longitude": [35.0]},
    )
    provinces = [ProvincePoint(province_id=f"{i:02d}", lat=39.0, lon=35.0) for i in range(1, 82)]

    frame = sample_daily_to_provinces(
        daily_grid=grid,
        provinces=provinces,
        source="cams_cams-europe-air-quality-reanalyses",
        run_id="cams_test",
    )

    assert len(frame) == 30 * 81
    assert frame["pm25_mean_ugm3"].notna().any()
    assert frame["no2_mean_ugm3"].notna().any()
    assert frame["o3_mean_ugm3"].notna().any()
    assert frame["air_quality_index"].iloc[0] == 30.0
