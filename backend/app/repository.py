from __future__ import annotations

from datetime import date

from sqlalchemy import Select, and_, case, desc, func, select
from sqlalchemy.orm import Session

from app.orm import AlertORM, DailyScoreORM, ProvinceORM


def get_latest_as_of_date(db: Session) -> date | None:
    return db.execute(select(func.max(DailyScoreORM.as_of_date))).scalar_one_or_none()


def _latest_scores_stmt() -> Select:
    latest = (
        select(
            DailyScoreORM.province_id,
            func.max(DailyScoreORM.as_of_date).label("max_date"),
        )
        .group_by(DailyScoreORM.province_id)
        .subquery()
    )

    return (
        select(ProvinceORM, DailyScoreORM)
        .join(DailyScoreORM, DailyScoreORM.province_id == ProvinceORM.id)
        .join(
            latest,
            and_(
                latest.c.province_id == DailyScoreORM.province_id,
                latest.c.max_date == DailyScoreORM.as_of_date,
            ),
        )
    )


def list_latest_province_scores(
    db: Session,
    *,
    region: str | None,
    min_score: int | None,
    risk_level: str | None,
    limit: int,
) -> list[tuple[ProvinceORM, DailyScoreORM]]:
    stmt = _latest_scores_stmt()

    if region:
        stmt = stmt.where(ProvinceORM.region == region)
    if risk_level:
        stmt = stmt.where(DailyScoreORM.risk_level == risk_level)
    if min_score is not None:
        stmt = stmt.where(DailyScoreORM.overall_score >= min_score)

    stmt = stmt.order_by(desc(DailyScoreORM.overall_score)).limit(limit)
    return list(db.execute(stmt).all())


def get_latest_province_score(db: Session, province_id: str) -> tuple[ProvinceORM, DailyScoreORM] | None:
    stmt = _latest_scores_stmt().where(ProvinceORM.id == province_id)
    return db.execute(stmt).first()


def list_active_alerts(
    db: Session,
    *,
    level: str | None,
    limit: int,
) -> list[tuple[AlertORM, ProvinceORM]]:
    stmt = (
        select(AlertORM, ProvinceORM)
        .join(ProvinceORM, ProvinceORM.id == AlertORM.province_id)
        .where(AlertORM.active.is_(True))
    )
    if level:
        stmt = stmt.where(AlertORM.level == level)

    severity_rank = case((AlertORM.level == "HIGH", 0), else_=1)
    stmt = stmt.order_by(severity_rank, desc(AlertORM.estimated_loss_usd), desc(AlertORM.issued_at)).limit(limit)
    return list(db.execute(stmt).all())
