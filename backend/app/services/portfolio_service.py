from __future__ import annotations

from datetime import date

from sqlalchemy import select

from app.database import SessionLocal
from app.orm import PortfolioAssetORM
from pipeline.risk_scoring import portfolio_risk_summary


def list_portfolios() -> list[dict]:
    with SessionLocal() as db:
        ids = db.execute(select(PortfolioAssetORM.portfolio_id).distinct().order_by(PortfolioAssetORM.portfolio_id)).all()
    return [{"portfolio_id": r[0], "name": r[0]} for r in ids]


def get_portfolio_risk_summary(portfolio_id: str, start: date, end: date) -> dict:
    out = portfolio_risk_summary(portfolio_id, start, end)
    return {
        "portfolio_id": portfolio_id,
        "period": {"start": start, "end": end},
        "bands": out.get("bands", {}),
        "peril_averages": out.get("peril_averages", {}),
        "top_assets": out.get("top_assets", []),
        "trend": out.get("trend", []),
    }

