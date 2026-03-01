from datetime import datetime

from sqlalchemy import text
from fastapi import APIRouter, HTTPException

from app.config import settings
from app.database import SessionLocal

router = APIRouter()


@router.get("/health", summary="Health check")
async def health():
    """Returns API health status and Postgres connectivity."""
    db_ok = False
    db_error: str | None = None

    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        db_error = str(exc)
    finally:
        if "session" in locals():
            session.close()

    if not db_ok:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "database": "unreachable",
                "error": db_error,
            },
        )

    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "model_version": settings.model_version,
        "confidence_score": settings.confidence_score,
        "services": {
            "database": "connected",
            "engine": settings.database_url.split(":")[0],
        },
    }
