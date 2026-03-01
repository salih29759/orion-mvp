from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from uuid import uuid4

import requests
from sqlalchemy import delete, select

from app.config import settings
from app.database import Base, SessionLocal, engine
from app.orm import AlertORM, DailyScoreORM, PipelineRunORM, ProvinceORM
from app.seed_data import TREND_CYCLE, TREND_PCTS, risk_level

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


@dataclass
class ScoreResult:
    province_id: str
    as_of_date: date
    flood_score: int
    drought_score: int
    overall_score: int
    risk_level: str
    trend: str
    trend_pct: float
    rain_7d_mm: float
    rain_60d_mm: float


def _rolling(values: list[float], window: int) -> list[float]:
    if len(values) < window:
        return []
    return [sum(values[i - window : i]) for i in range(window, len(values) + 1)]


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _fetch_precip(lat: float, lng: float, start: date, end: date) -> list[float]:
    params = {
        "latitude": lat,
        "longitude": lng,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": "precipitation_sum",
        "timezone": "UTC",
    }
    res = requests.get(ARCHIVE_URL, params=params, timeout=30)
    res.raise_for_status()
    data = res.json().get("daily", {})
    rain = data.get("precipitation_sum", [])
    return [float(v or 0.0) for v in rain]


def _score_for_province(province: ProvinceORM, history: list[float], as_of_date: date) -> ScoreResult:
    # last 30 days expected (backfill calls may provide less)
    rain_7d = sum(history[-7:]) if len(history) >= 7 else sum(history)
    rain_60d = sum(history[-60:]) if len(history) >= 60 else sum(history)

    rolling_7d = _rolling(history, 7)
    baseline_7d = mean(rolling_7d[:-1]) if len(rolling_7d) > 1 else (rolling_7d[-1] if rolling_7d else 1.0)
    flood_ratio = rain_7d / max(1.0, baseline_7d)
    flood_score = int(round(_clamp((flood_ratio - 0.5) * 80, 0, 100)))

    # Drought: compare current 60-day rainfall to trailing historical average
    if len(history) >= 120:
        baseline_60d = mean([sum(history[i - 60 : i]) for i in range(60, len(history) - 1)])
    else:
        baseline_60d = max(1.0, rain_60d)

    deficit = _clamp(1 - (rain_60d / max(1.0, baseline_60d)), 0, 1)
    drought_score = int(round(_clamp(deficit * 120, 0, 100)))

    overall = int(round(0.65 * flood_score + 0.35 * drought_score))
    plate = int(province.id)

    return ScoreResult(
        province_id=province.id,
        as_of_date=as_of_date,
        flood_score=flood_score,
        drought_score=drought_score,
        overall_score=overall,
        risk_level=risk_level(overall),
        trend=TREND_CYCLE[plate % 3],
        trend_pct=round(TREND_PCTS[plate % len(TREND_PCTS)], 1),
        rain_7d_mm=round(rain_7d, 1),
        rain_60d_mm=round(rain_60d, 1),
    )


def _upsert_score(db, score: ScoreResult) -> None:
    existing = db.execute(
        select(DailyScoreORM).where(
            DailyScoreORM.province_id == score.province_id,
            DailyScoreORM.as_of_date == score.as_of_date,
        )
    ).scalar_one_or_none()

    payload = {
        "flood_score": score.flood_score,
        "drought_score": score.drought_score,
        "overall_score": score.overall_score,
        "risk_level": score.risk_level,
        "trend": score.trend,
        "trend_pct": score.trend_pct,
        "rain_7d_mm": score.rain_7d_mm,
        "rain_60d_mm": score.rain_60d_mm,
        "data_source": settings.default_data_source,
        "model_version": settings.model_version,
    }

    if existing:
        for k, v in payload.items():
            setattr(existing, k, v)
        return

    db.add(
        DailyScoreORM(
            province_id=score.province_id,
            as_of_date=score.as_of_date,
            **payload,
        )
    )


def _refresh_alerts(db, as_of_date: date) -> int:
    db.execute(delete(AlertORM).where(AlertORM.active.is_(True)))

    rows = db.execute(
        select(ProvinceORM, DailyScoreORM)
        .join(DailyScoreORM, DailyScoreORM.province_id == ProvinceORM.id)
        .where(DailyScoreORM.as_of_date == as_of_date)
    ).all()

    now = datetime.now(timezone.utc)
    count = 0
    for province, score in rows:
        if score.flood_score >= 85:
            db.add(
                AlertORM(
                    id=f"flood-{province.id}-{as_of_date}",
                    province_id=province.id,
                    level="HIGH",
                    risk_type="FLOOD",
                    affected_policies=1200 + score.flood_score * 10,
                    estimated_loss_usd=750000 + score.flood_score * 17500,
                    message=f"Heavy rainfall anomaly for {province.name}. 7-day rainfall: {score.rain_7d_mm:.1f} mm.",
                    issued_at=now,
                    active=True,
                )
            )
            count += 1

        if score.drought_score >= 60:
            level = "HIGH" if score.drought_score >= 75 else "MEDIUM"
            db.add(
                AlertORM(
                    id=f"drought-{province.id}-{as_of_date}",
                    province_id=province.id,
                    level=level,
                    risk_type="DROUGHT",
                    affected_policies=900 + score.drought_score * 9,
                    estimated_loss_usd=500000 + score.drought_score * 12000,
                    message=f"60-day rainfall deficit elevated in {province.name}. Drought score: {score.drought_score}.",
                    issued_at=now,
                    active=True,
                )
            )
            count += 1
    return count


def run(backfill_days: int = 30) -> None:
    Base.metadata.create_all(bind=engine)

    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    status = "success"
    rows_written = 0
    error = None

    with SessionLocal() as db:
        db.add(PipelineRunORM(run_id=run_id, started_at=started_at, status="running", rows_written=0))
        db.commit()

    try:
        with SessionLocal() as db:
            provinces = db.execute(select(ProvinceORM)).scalars().all()
            if not provinces:
                raise RuntimeError("No provinces found. Run scripts/seed_postgres.py first.")

            today = date.today()
            start = today - timedelta(days=max(120, backfill_days + 90))

            for province in provinces:
                history = _fetch_precip(province.lat, province.lng, start, today)
                if len(history) < 30:
                    continue

                for offset in range(backfill_days):
                    as_of = today - timedelta(days=(backfill_days - 1 - offset))
                    slice_end = len(history) - (backfill_days - 1 - offset)
                    candidate = history[:slice_end]
                    score = _score_for_province(province, candidate, as_of)
                    _upsert_score(db, score)
                    rows_written += 1

            _refresh_alerts(db, today)
            db.commit()

    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error = str(exc)
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        with SessionLocal() as db:
            run_row = db.get(PipelineRunORM, run_id)
            if run_row:
                run_row.finished_at = finished_at
                run_row.status = status
                run_row.rows_written = rows_written
                run_row.error = error
                db.commit()


if __name__ == "__main__":
    run(backfill_days=30)
