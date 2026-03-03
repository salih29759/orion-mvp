from __future__ import annotations

from fastapi import APIRouter, Depends

from app.auth import verify_token
from app.errors import ApiError
from app.schemas.cams import CamsBackfillAcceptedResponse, CamsBackfillRequest, CamsBackfillStatusResponse
from app.services.cams_job_service import create_backfill, get_latest_status

router = APIRouter()


@router.post("/jobs/cams/backfill", response_model=CamsBackfillAcceptedResponse, status_code=202, tags=["Jobs"])
async def cams_backfill(body: CamsBackfillRequest, _: str = Depends(verify_token)):
    try:
        return create_backfill(start=body.start, end=body.end, concurrency=body.concurrency, force=body.force)
    except ValueError as exc:
        raise ApiError(status_code=400, error_code="VALIDATION_ERROR", message=str(exc)) from exc
    except RuntimeError as exc:
        status_code = 400 if "No CAMS data available" in str(exc) else 503
        raise ApiError(status_code=status_code, error_code="CAMS_BACKFILL_ERROR", message=str(exc)) from exc


@router.get("/jobs/cams/status", response_model=CamsBackfillStatusResponse, tags=["Jobs"])
async def cams_backfill_status(run_id: str | None = None, _: str = Depends(verify_token)):
    return get_latest_status(run_id=run_id)
