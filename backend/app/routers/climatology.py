from __future__ import annotations

from fastapi import APIRouter, Depends

from app.auth import verify_token
from app.schemas.common import ClimatologyBuildRequest, ClimatologyBuildResponse
from app.services.job_service import build_climatology_job

router = APIRouter()


@router.post("/climatology/build", response_model=ClimatologyBuildResponse, tags=["Climatology"])
async def build_climatology(body: ClimatologyBuildRequest, _: str = Depends(verify_token)):
    return build_climatology_job(
        version=body.version,
        baseline_start=body.baseline_start,
        baseline_end=body.baseline_end,
        level=body.level,
    )

