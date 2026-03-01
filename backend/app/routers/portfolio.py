from types import SimpleNamespace
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException

from app.auth import verify_token
from app.database import get_db
from app.models import (
    HazardExposure,
    PortfolioAnalysis,
    PortfolioAnalyzeRequest,
    PortfolioAnalyzeResponse,
    RiskLevel,
)
from app.repository import get_latest_province_score

router = APIRouter()

# Vulnerability factors: fraction of sum_insured at risk per unit of score
_FLOOD_FACTOR = 0.0015      # 0.15 % per score point → 15 % at score=100
_DROUGHT_FACTOR = 0.0008    # 0.08 % per score point →  8 % at score=100
_PREMIUM_LOADING = 1.40     # 40 % loading (expenses + profit margin)


def _build_recommendations(province: SimpleNamespace) -> list[str]:
    recs: list[str] = []

    if province.flood_score >= 75:
        recs.append("Enforce mandatory flood cover for all policies in this province.")
        recs.append("Require elevation certificates for properties below 5 m AMSL.")
    elif province.flood_score >= 50:
        recs.append("Apply a flood sub-limit of 50 % of sum insured for coastal assets.")

    if province.drought_score >= 75:
        recs.append("Implement agricultural drought riders for farm-related policies.")
        recs.append("Consider reinsurance treaty for drought aggregate exposure.")
    elif province.drought_score >= 50:
        recs.append("Monitor soil-moisture indices quarterly for early drought warning.")

    if province.risk_level == RiskLevel.HIGH:
        recs.append("Restrict new policy issuance in HIGH-risk zones pending re-underwriting.")
        recs.append("Increase deductible floors by 25 % for residential properties.")
    elif province.risk_level == RiskLevel.MEDIUM:
        recs.append("Schedule annual portfolio review for this province.")

    if province.trend == "UP":
        recs.append("Risk trend is increasing — consider rate adjustment at next renewal.")

    if not recs:
        recs.append("No immediate action required. Continue standard monitoring.")

    return recs


@router.post(
    "/analyze",
    response_model=PortfolioAnalyzeResponse,
    summary="Analyze exposure for a portfolio in a given province",
)
async def analyze_portfolio(
    body: PortfolioAnalyzeRequest,
    db: Session = Depends(get_db),
    _: str = Depends(verify_token),
):
    row = get_latest_province_score(db, body.province_id)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Province '{body.province_id}' not found. Use the plate number as ID (e.g. '34' for İstanbul).",
        )
    province, score = row
    p = SimpleNamespace(
        id=province.id,
        name=province.name,
        risk_level=score.risk_level,
        flood_score=score.flood_score,
        drought_score=score.drought_score,
        trend=score.trend,
    )

    flood_loss = body.sum_insured * p.flood_score * _FLOOD_FACTOR
    drought_loss = body.sum_insured * p.drought_score * _DROUGHT_FACTOR
    total_loss = flood_loss + drought_loss
    combined_lr = total_loss / body.sum_insured
    suggested_premium = total_loss * _PREMIUM_LOADING

    analysis = PortfolioAnalysis(
        province_id=p.id,
        province_name=p.name,
        risk_level=p.risk_level,
        policy_count=body.policy_count,
        sum_insured=body.sum_insured,
        flood=HazardExposure(
            score=p.flood_score,
            expected_loss_usd=round(flood_loss, 2),
            loss_ratio=round(flood_loss / body.sum_insured, 4),
        ),
        drought=HazardExposure(
            score=p.drought_score,
            expected_loss_usd=round(drought_loss, 2),
            loss_ratio=round(drought_loss / body.sum_insured, 4),
        ),
        total_expected_loss_usd=round(total_loss, 2),
        combined_loss_ratio=round(combined_lr, 4),
        suggested_annual_premium_usd=round(suggested_premium, 2),
        recommendations=_build_recommendations(p),
    )

    return PortfolioAnalyzeResponse(data=analysis)
