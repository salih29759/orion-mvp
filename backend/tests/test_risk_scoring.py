from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.orm import ClimatologyThresholdORM
from pipeline import risk_scoring


def test_temp_conversion_kelvin_to_celsius():
    s = pd.Series([273.15, 283.15, 293.15])
    out = risk_scoring.temp_to_celsius(s)
    assert round(float(out.iloc[0]), 2) == 0.0
    assert round(float(out.iloc[2]), 2) == 20.0


def test_precip_conversion_m_to_mm():
    s = pd.Series([0.001, 0.01, 0.1])
    out = risk_scoring.precip_to_mm(s)
    assert round(float(out.iloc[0]), 3) == 1.0
    assert round(float(out.iloc[2]), 3) == 100.0


def test_threshold_retrieval_with_nearest_fallback(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(risk_scoring, "SessionLocal", TestSession)

    with TestSession() as db:
        db.add(
            ClimatologyThresholdORM(
                climatology_version="vtest",
                cell_lat=41.0,
                cell_lng=29.0,
                month=1,
                temp_max_p95=30.0,
                wind_max_p95=10.0,
                precip_1d_p95=20.0,
                precip_1d_p99=40.0,
                precip_7d_p95=50.0,
                precip_7d_p99=80.0,
                precip_30d_p10=15.0,
                soil_moisture_p10=0.15,
            )
        )
        db.commit()

    got = risk_scoring.get_thresholds(41.02, 28.98, date(2024, 1, 5), "vtest")
    assert got is not None
    assert got["temp_max_p95"] == 30.0
    assert got["soil_moisture_p10"] == 0.15


def test_score_direction_higher_is_worse():
    assert risk_scoring.score_heat(2, 0.5) < risk_scoring.score_heat(12, 3.0)
    assert risk_scoring.score_rain(20, 10, 50, 80, 40) < risk_scoring.score_rain(120, 70, 50, 80, 40)
    assert risk_scoring.score_wind(2, 8, 10) < risk_scoring.score_wind(12, 18, 10)
    assert risk_scoring.score_drought(60, 20, 0.4, 0.15) < risk_scoring.score_drought(5, 20, 0.05, 0.15)

