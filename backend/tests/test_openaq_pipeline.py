from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.orm import OpenaqIngestJobORM
from pipeline import openaq_pipeline


def _build_sessionmaker():
    engine = create_engine("sqlite:///:memory:", future=True)
    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)
    return TestSession


def test_signature_dedupe(monkeypatch):
    TestSession = _build_sessionmaker()
    monkeypatch.setattr(openaq_pipeline, "SessionLocal", TestSession)
    monkeypatch.setattr(openaq_pipeline, "_utc_today", lambda: date(2026, 3, 2))

    job_id_1, dedup_1, months_1 = openaq_pipeline.submit_openaq_backfill(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        concurrency=5,
    )
    job_id_2, dedup_2, months_2 = openaq_pipeline.submit_openaq_backfill(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        concurrency=5,
    )

    assert dedup_1 is False
    assert dedup_2 is True
    assert job_id_1 == job_id_2
    assert months_1 == months_2 == 1

    with TestSession() as db:
        rows = db.execute(select(OpenaqIngestJobORM)).scalars().all()
    assert len(rows) == 1


def test_end_date_clamp(monkeypatch):
    TestSession = _build_sessionmaker()
    monkeypatch.setattr(openaq_pipeline, "SessionLocal", TestSession)
    monkeypatch.setattr(openaq_pipeline, "_utc_today", lambda: date(2026, 3, 2))

    job_id, _dedup, months_total = openaq_pipeline.submit_openaq_backfill(
        start_date=date(2026, 2, 15),
        end_date=date(2026, 12, 31),
        concurrency=5,
    )
    assert months_total == 2

    with TestSession() as db:
        row = db.get(OpenaqIngestJobORM, job_id)
    assert row is not None
    assert row.effective_end_date == date(2026, 3, 1)


@pytest.mark.parametrize("unit", ["ug/m3", "µg/m³", "μg/m3"])
def test_unit_filter_accepts_ugm3_variants(unit):
    parsed, reason = openaq_pipeline._measurement_to_row(
        {
            "value": 12.3,
            "flagInfo": {"hasFlags": False},
            "parameter": {"name": "pm25", "units": unit},
            "period": {"datetimeFrom": {"utc": "2026-01-01T00:15:00Z"}},
        },
        default_parameter="pm25",
    )
    assert reason is None
    assert parsed is not None


def test_unit_filter_skips_non_ugm3():
    parsed, reason = openaq_pipeline._measurement_to_row(
        {
            "value": 33.1,
            "flagInfo": {"hasFlags": False},
            "parameter": {"name": "no2", "units": "ppb"},
            "period": {"datetimeFrom": {"utc": "2026-01-01T00:15:00Z"}},
        },
        default_parameter="no2",
    )
    assert parsed is None
    assert reason == "non_ugm3"


def test_flagged_measurement_skipped():
    parsed, reason = openaq_pipeline._measurement_to_row(
        {
            "value": 44.0,
            "flagInfo": {"hasFlags": True},
            "parameter": {"name": "o3", "units": "ug/m3"},
            "period": {"datetimeFrom": {"utc": "2026-01-01T00:15:00Z"}},
        },
        default_parameter="o3",
    )
    assert parsed is None
    assert reason == "flagged"


def test_hourly_union_coverage_and_daily_means():
    station = openaq_pipeline.StationInfo(
        station_id="101",
        name="Test",
        lat=39.9,
        lon=32.8,
        province_id="6",
        sensors={"pm10": [], "pm25": [1], "no2": [2], "o3": [3]},
    )
    rows = [
        {"parameter": "pm25", "hour": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "value": 10.0},
        {"parameter": "no2", "hour": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc), "value": 20.0},
        {"parameter": "o3", "hour": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc), "value": 30.0},
        {"parameter": "pm25", "hour": datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc), "value": 14.0},
    ]

    out = openaq_pipeline._aggregate_station_day_rows(
        station,
        rows,
        ingested_at=datetime(2026, 1, 2, 8, 0, tzinfo=timezone.utc),
    )
    assert len(out) == 1
    row = out[0]
    assert row["measurement_count"] == 2
    assert row["coverage_pct"] == pytest.approx((2 / 24.0) * 100.0)
    assert row["pm25_measured_ugm3"] == pytest.approx(12.0)
    assert row["no2_measured_ugm3"] == pytest.approx(20.0)
    assert row["o3_measured_ugm3"] == pytest.approx(30.0)


def test_nearest_province_mapping():
    provinces = [
        ("34", 41.0, 29.0),
        ("6", 39.93, 32.85),
        ("35", 38.42, 27.14),
    ]
    province_id = openaq_pipeline._nearest_province_id(39.92, 32.86, provinces)
    assert province_id == "6"


def test_station_day_pivot_columns_present():
    station = openaq_pipeline.StationInfo(
        station_id="202",
        name="Only-NO2",
        lat=38.4,
        lon=27.1,
        province_id="35",
        sensors={"pm10": [], "pm25": [], "no2": [2], "o3": []},
    )
    rows = [
        {"parameter": "no2", "hour": datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc), "value": 18.5},
    ]
    out = openaq_pipeline._aggregate_station_day_rows(
        station,
        rows,
        ingested_at=datetime(2026, 1, 6, 8, 0, tzinfo=timezone.utc),
    )
    assert len(out) == 1
    row = out[0]
    assert set(row.keys()) == {
        "date",
        "station_id",
        "province_id",
        "lat",
        "lon",
        "pm25_measured_ugm3",
        "no2_measured_ugm3",
        "o3_measured_ugm3",
        "measurement_count",
        "coverage_pct",
        "source",
        "ingested_at",
    }
    assert row["pm25_measured_ugm3"] is None
    assert row["no2_measured_ugm3"] == pytest.approx(18.5)
    assert row["o3_measured_ugm3"] is None
