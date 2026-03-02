from sqlalchemy import text
from fastapi import APIRouter

from app.config import settings
from app.database import SessionLocal
from app.errors import ApiError
from app.schemas.common import HealthResponse, MetricsResponse
from app.services.job_service import get_metrics_payload

router = APIRouter()


@router.get("/health", response_model=HealthResponse, summary="Health check")
async def health():
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
    except Exception as exc:
        raise ApiError(
            status_code=503,
            error_code="SERVICE_UNAVAILABLE",
            message="Database is unreachable",
            details={"error": str(exc)},
        ) from exc
    finally:
        if "session" in locals():
            session.close()

    return HealthResponse(status="ok", version=settings.model_version)


@router.get("/health/metrics", response_model=MetricsResponse, summary="Basic operational metrics")
async def metrics():
    return get_metrics_payload()
