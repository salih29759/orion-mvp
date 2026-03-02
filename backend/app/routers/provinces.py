from sqlalchemy.orm import Session
import logging
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth import verify_token
from app.database import get_db
from app.models import Province, ProvinceDetailResponse, ProvincesResponse, RiskLevel
from app.repository import get_latest_as_of_date, get_latest_province_score, list_latest_province_scores

router = APIRouter()
LOG = logging.getLogger("orion.deprecation")


def _to_province(payload: tuple) -> Province:
    province, score = payload
    return Province(
        id=province.id,
        plate=province.plate,
        name=province.name,
        region=province.region,
        lat=province.lat,
        lng=province.lng,
        population=province.population,
        insured_assets=province.insured_assets,
        flood_score=score.flood_score,
        drought_score=score.drought_score,
        overall_score=score.overall_score,
        risk_level=score.risk_level,
        trend=score.trend,
        trend_pct=score.trend_pct,
        rain_7d_mm=score.rain_7d_mm,
        rain_60d_mm=score.rain_60d_mm,
        data_source=score.data_source,
        as_of_date=score.as_of_date,
    )


@router.get(
    "/provinces",
    response_model=ProvincesResponse,
    summary="List all provinces with risk scores",
)
async def list_provinces(
    region: str | None = Query(None, description="Filter by region name (e.g. Karadeniz, Marmara)"),
    min_score: int | None = Query(None, ge=0, le=100, description="Minimum overall_score"),
    risk_level: RiskLevel | None = Query(None, description="Filter by risk level"),
    limit: int = Query(81, ge=1, le=81, description="Max results to return"),
    db: Session = Depends(get_db),
    _: str = Depends(verify_token),
):
    LOG.warning("legacy_endpoint_used path=/v1/risk/provinces")
    rows = list_latest_province_scores(
        db,
        region=region,
        min_score=min_score,
        risk_level=risk_level.value if risk_level else None,
        limit=limit,
    )
    provinces = [_to_province(r) for r in rows]
    as_of_date = get_latest_as_of_date(db)
    data_source = provinces[0].data_source if provinces else None

    return ProvincesResponse(
        data=provinces,
        as_of_date=as_of_date,
        data_source=data_source,
        pagination={"total": len(provinces), "returned": len(provinces)},
    )


@router.get(
    "/provinces/{province_id}",
    response_model=ProvinceDetailResponse,
    summary="Get a single province by plate number or slug",
)
async def get_province(
    province_id: str,
    db: Session = Depends(get_db),
    _: str = Depends(verify_token),
):
    LOG.warning("legacy_endpoint_used path=/v1/risk/provinces/{province_id}")
    row = get_latest_province_score(db, province_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Province '{province_id}' not found")
    p = _to_province(row)
    return ProvinceDetailResponse(data=p, as_of_date=p.as_of_date, data_source=p.data_source)
