from __future__ import annotations

from datetime import date

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.orm import NasaIngestJobORM
from pipeline import nasa_earthdata


def test_decode_smap_retrieval_flag_mapping():
    assert nasa_earthdata.decode_smap_retrieval_flag(None) == 2
    assert nasa_earthdata.decode_smap_retrieval_flag(0) == 0
    assert nasa_earthdata.decode_smap_retrieval_flag(1) == 0
    assert nasa_earthdata.decode_smap_retrieval_flag(2) == 2
    assert nasa_earthdata.decode_smap_retrieval_flag(3) == 1


def test_modis_land_valid_mask():
    qa = np.array([0, 1, 2, 3], dtype=np.int32)
    out = nasa_earthdata.modis_land_valid_mask(qa)
    assert out.tolist() == [False, False, False, True]


def test_modis_tile_rowcol_to_latlon_bounds():
    rows = np.array([0, 1200, 2399], dtype=np.int32)
    cols = np.array([0, 1200, 2399], dtype=np.int32)
    lat, lon = nasa_earthdata.modis_tile_rowcol_to_latlon(21, 9, rows, cols)
    assert np.isfinite(lat).all()
    assert np.isfinite(lon).all()
    assert (lat >= -90).all() and (lat <= 90).all()
    assert (lon >= -180).all() and (lon <= 180).all()


def test_submit_nasa_backfill_dedup_and_retry_after_failure(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)

    monkeypatch.setattr(nasa_earthdata, "SessionLocal", TestSession)
    monkeypatch.setattr(nasa_earthdata, "start_nasa_background_job", lambda _job_id: None)

    args = {"dataset": "smap", "start_date": date(2024, 1, 1), "end_date": date(2024, 1, 31)}
    job1, dedup1, months1 = nasa_earthdata.submit_nasa_backfill(**args)
    job2, dedup2, months2 = nasa_earthdata.submit_nasa_backfill(**args)

    assert dedup1 is False
    assert dedup2 is True
    assert job1 == job2
    assert months1 == 1
    assert months2 == 1

    with TestSession() as db:
        row = db.get(NasaIngestJobORM, job1)
        assert row is not None
        row.status = "failed"
        db.commit()

    job3, dedup3, months3 = nasa_earthdata.submit_nasa_backfill(**args)
    assert dedup3 is False
    assert job3 != job1
    assert months3 == 1


def test_get_latest_nasa_jobs_prefers_active(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(nasa_earthdata, "SessionLocal", TestSession)

    with TestSession() as db:
        db.add(
            NasaIngestJobORM(
                job_id="smap-success",
                request_signature="sig-smap-success",
                dataset="smap",
                status="success",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                months_total=1,
                months_completed=1,
                months_failed=0,
                rows_written=81,
                files_downloaded=1,
                files_written=1,
            )
        )
        db.add(
            NasaIngestJobORM(
                job_id="smap-running",
                request_signature="sig-smap-running",
                dataset="smap",
                status="running",
                start_date=date(2024, 2, 1),
                end_date=date(2024, 2, 29),
                months_total=1,
                months_completed=0,
                months_failed=0,
                rows_written=0,
                files_downloaded=0,
                files_written=0,
            )
        )
        db.commit()

    latest = nasa_earthdata.get_latest_nasa_jobs()
    assert latest["smap"] is not None
    assert latest["smap"].job_id == "smap-running"
