"""Seed Postgres with Turkish province metadata only.

Usage:
  python scripts/seed_postgres.py --dry-run
  python scripts/seed_postgres.py
"""

from __future__ import annotations

import argparse
import os
import sys

from sqlalchemy import delete

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import Base, SessionLocal, engine
from app.orm import AlertORM, DailyScoreORM, ProvinceORM
from app.seed_data import RAW_PROVINCES


def run(dry_run: bool) -> None:
    Base.metadata.create_all(bind=engine)

    provinces = [
        ProvinceORM(
            id=str(plate),
            plate=plate,
            name=name,
            region=region,
            lat=lat,
            lng=lng,
            population=pop,
            insured_assets=pop * 18000,
        )
        for plate, name, region, lat, lng, _flood, _drought, pop in RAW_PROVINCES
    ]
    print(f"Provinces: {len(provinces)}")

    if dry_run:
        print("Dry run complete. No writes performed.")
        return

    with SessionLocal() as db:
        db.execute(delete(AlertORM))
        db.execute(delete(DailyScoreORM))
        db.execute(delete(ProvinceORM))

        db.add_all(provinces)
        db.commit()

    print("Province metadata seed completed.")
    print("Next step: run `python -m pipeline.run_pipeline` to pull real API data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
