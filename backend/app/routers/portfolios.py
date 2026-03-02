from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from app.auth import verify_token
from app.errors import ApiError
from app.schemas.portfolio import Portfolio, RiskSummaryResponse
from app.services.portfolio_service import get_portfolio_risk_summary, list_portfolios

router = APIRouter()


@router.get("/portfolios", response_model=list[Portfolio], tags=["Portfolios"])
async def get_portfolios(_: str = Depends(verify_token)):
    return list_portfolios()


@router.get("/portfolios/{portfolio_id}/risk-summary", response_model=RiskSummaryResponse, tags=["Portfolios"])
async def get_risk_summary(
    portfolio_id: str,
    start: date = Query(...),
    end: date = Query(...),
    _: str = Depends(verify_token),
):
    if start > end:
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message="start must be <= end")
    return get_portfolio_risk_summary(portfolio_id=portfolio_id, start=start, end=end)
