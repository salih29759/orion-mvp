from __future__ import annotations

from datetime import date
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.orm import Era5BackfillItemORM, Era5IngestJobORM
from pipeline import era5_ingestion


def test_backfill_idempotency(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(era5_ingestion, "SessionLocal", TestSession)
    monkeypatch.setattr(era5_ingestion, "kick_queued_jobs", lambda: 0)

    def fake_submit(req, enforce_limit=True):
        job_id = str(uuid4())
        with TestSession() as db:
            db.add(
                Era5IngestJobORM(
                    job_id=job_id,
                    request_signature=f"sig-{job_id}",
                    status="queued",
                    dataset=req.dataset,
                    variables_csv=",".join(req.variables),
                    bbox_csv=",".join(str(x) for x in req.bbox),
                    start_date=req.start_date,
                    end_date=req.end_date,
                )
            )
            db.commit()
        return job_id, False

    monkeypatch.setattr(era5_ingestion, "submit_era5_job", fake_submit)

    args = {
        "start_month": "2015-01",
        "end_month": "2015-03",
        "bbox": (42.0, 26.0, 36.0, 45.0),
        "variables": [
            "2m_temperature",
            "total_precipitation",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "volumetric_soil_water_layer_1",
        ],
        "mode": "monthly",
        "dataset": "era5-land",
        "concurrency": 2,
    }

    backfill_id_1, dedup_1, months_1 = era5_ingestion.submit_backfill(**args)
    backfill_id_2, dedup_2, months_2 = era5_ingestion.submit_backfill(**args)

    assert dedup_1 is False
    assert dedup_2 is True
    assert backfill_id_1 == backfill_id_2
    assert months_1 == 3
    assert months_2 == 3

    with TestSession() as db:
        items = db.execute(select(Era5BackfillItemORM)).scalars().all()
    assert len(items) == 3

