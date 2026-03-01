"""Seed Postgres with Turkish provinces and initial risk/alert data.

Usage:
  python scripts/seed_postgres.py --dry-run
  python scripts/seed_postgres.py
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import argparse
import os
import sys

from sqlalchemy import delete

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.database import Base, SessionLocal, engine
from app.orm import AlertORM, DailyScoreORM, ProvinceORM
from app.seed_data import RAW_PROVINCES, TREND_CYCLE, TREND_PCTS, risk_level


def _seed_scores_for_today() -> list[DailyScoreORM]:
    rows: list[DailyScoreORM] = []
    today = date.today()
    for plate, _name, _region, _lat, _lng, flood, drought, _pop in RAW_PROVINCES:
        overall = max(flood, drought)
        rows.append(
            DailyScoreORM(
                province_id=str(plate),
                as_of_date=today,
                flood_score=flood,
                drought_score=drought,
                overall_score=overall,
                risk_level=risk_level(overall),
                trend=TREND_CYCLE[plate % 3],
                trend_pct=round(TREND_PCTS[plate % len(TREND_PCTS)], 1),
                rain_7d_mm=float(flood * 2.1),
                rain_60d_mm=float((100 - drought) * 8.0),
                data_source=settings.default_data_source,
                model_version=settings.model_version,
            )
        )
    return rows


def _seed_alerts(scores: list[DailyScoreORM]) -> list[AlertORM]:
    now = datetime.now(timezone.utc)
    score_by_id = {s.province_id: s for s in scores}
    alerts: list[AlertORM] = []
    for province_id, score in score_by_id.items():
        if score.flood_score >= 85:
            alerts.append(
                AlertORM(
                    id=f"flood-{province_id}-{score.as_of_date}",
                    province_id=province_id,
                    level="HIGH",
                    risk_type="FLOOD",
                    affected_policies=1200 + int(score.flood_score * 13),
                    estimated_loss_usd=750000 + score.flood_score * 18000,
                    message=f"Heavy rainfall anomaly detected. 7-day rainfall reached {score.rain_7d_mm:.1f} mm.",
                    issued_at=now,
                    active=True,
                )
            )
        if score.drought_score >= 60:
            level = "HIGH" if score.drought_score >= 75 else "MEDIUM"
            alerts.append(
                AlertORM(
                    id=f"drought-{province_id}-{score.as_of_date}",
                    province_id=province_id,
                    level=level,
                    risk_type="DROUGHT",
                    affected_policies=900 + int(score.drought_score * 11),
                    estimated_loss_usd=500000 + score.drought_score * 15000,
                    message=f"60-day rainfall deficit elevated. Current drought score is {score.drought_score}.",
                    issued_at=now,
                    active=True,
                )
            )
    return alerts


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
    scores = _seed_scores_for_today()
    alerts = _seed_alerts(scores)

    print(f"Database URL: {settings.database_url}")
    print(f"Provinces: {len(provinces)} | Scores: {len(scores)} | Alerts: {len(alerts)}")

    if dry_run:
        print("Dry run complete. No writes performed.")
        return

    with SessionLocal() as db:
        db.execute(delete(AlertORM))
        db.execute(delete(DailyScoreORM))
        db.execute(delete(ProvinceORM))

        db.add_all(provinces)
        db.add_all(scores)
        db.add_all(alerts)
        db.commit()

    print("Seed completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
