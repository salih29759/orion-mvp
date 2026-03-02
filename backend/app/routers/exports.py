from __future__ import annotations

from fastapi import APIRouter, Depends

from app.auth import verify_token
from app.schemas.exports import ExportPortfolioRequest, ExportPortfolioResponse
from app.services.export_service import create_portfolio_export

router = APIRouter()


@router.post("/export/portfolio", response_model=ExportPortfolioResponse, tags=["Exports"])
async def export_portfolio(body: ExportPortfolioRequest, _: str = Depends(verify_token)):
    return create_portfolio_export(
        portfolio_id=body.portfolio_id,
        start_date=body.start_date,
        end_date=body.end_date,
        include_drivers=body.include_drivers,
    )

