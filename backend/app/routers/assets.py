from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.auth import verify_token
from app.schemas.common import AssetWildfireFeaturesResponse
from app.services.job_service import get_wildfire_features

router = APIRouter()


@router.get("/assets/{asset_id}/wildfire-features", response_model=AssetWildfireFeaturesResponse, tags=["Assets", "FIRMS"])
async def wildfire_features(asset_id: str, window: str = Query("24h", pattern="^(24h|7d)$"), _: str = Depends(verify_token)):
    return get_wildfire_features(asset_id=asset_id, window=window)

