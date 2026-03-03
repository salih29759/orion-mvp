from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.orm import GlofasBackfillItemORM
from pipeline import glofas_pipeline


def test_glofas_backfill_idempotency(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    test_session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)

    monkeypatch.setattr(glofas_pipeline, "SessionLocal", test_session)
    monkeypatch.setattr(glofas_pipeline, "start_glofas_background_job", lambda _job_id: None)

    args = {
        "start": date(1979, 1, 1),
        "end": date(1979, 3, 31),
        "concurrency": 2,
    }

    job_id_1, dedup_1, months_1 = glofas_pipeline.submit_glofas_backfill(**args)
    job_id_2, dedup_2, months_2 = glofas_pipeline.submit_glofas_backfill(**args)

    assert dedup_1 is False
    assert dedup_2 is True
    assert job_id_1 == job_id_2
    assert months_1 == 3
    assert months_2 == 3

    with test_session() as db:
        items = db.execute(select(GlofasBackfillItemORM)).scalars().all()
    assert len(items) == 3
