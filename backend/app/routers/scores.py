from __future__ import annotations

from fastapi import APIRouter, Depends

from app.auth import verify_token
from app.schemas.scores import BatchScoresRequest, BatchScoresResponse
from app.services.scoring_service import run_batch_scores

router = APIRouter()


@router.post("/scores/batch", response_model=BatchScoresResponse, tags=["Scores"])
async def batch_scores(body: BatchScoresRequest, _: str = Depends(verify_token)):
    assets = [{"asset_id": a.asset_id, "lat": a.lat, "lon": a.lon} for a in body.assets]
    return run_batch_scores(
        assets=assets,
        start_date=body.start_date,
        end_date=body.end_date,
        climatology_version=body.climatology_version,
        include_perils=body.include_perils,
    )

