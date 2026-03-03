from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.database import Base, engine
from app.database import SessionLocal
from app.bootstrap import ensure_ops_schema, ensure_provinces_seeded
from app.errors import ApiError
from app import orm  # noqa: F401
from app.routers import (
    alerts,
    assets,
    aws_jobs,
    climatology,
    era5_ops,
    exports,
    firms,
    health,
    internal,
    jobs,
    notifications,
    portfolio,
    portfolios,
    provinces,
    sentinel_jobs,
    scores,
)

app = FastAPI(
    title="Orion Labs Climate Risk API",
    description=(
        "Climate risk intelligence platform for Turkish insurance portfolios.\n\n"
        "## Authentication\n"
        "All `/v1/*` endpoints require a Bearer token:\n"
        "```\nAuthorization: Bearer <api_key>\n```\n\n"
        "## Risk scoring\n"
        "Scores range from **0 – 100**. Risk levels: `HIGH ≥ 75`, `MEDIUM ≥ 50`, `LOW < 50`.\n\n"
        "## Rate limits\n"
        "| Plan | Requests/min | Provinces | History |\n"
        "|------|-------------|-----------|--------|\n"
        "| Starter | 60 | 10 | 30 days |\n"
        "| Pro | 300 | 81 | 1 year |\n"
        "| Enterprise | Unlimited | 81 | 5 years |\n"
    ),
    version="2.1.0",
    contact={"name": "Orion Labs", "email": "api@orionlabs.io"},
    license_info={"name": "Proprietary — Authorized use only"},
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "Health", "description": "Service health & readiness probes"},
        {"name": "Risk", "description": "Province-level climate risk scores"},
        {"name": "Alerts", "description": "Active climate event alerts"},
        {"name": "Portfolio", "description": "Portfolio exposure analysis"},
        {"name": "Jobs", "description": "Asynchronous ingestion and climate feature jobs"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.allowed_origins.split(",") if origin.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(ApiError)
async def api_error_handler(_, exc: ApiError):
    payload = {"error_code": exc.error_code, "message": exc.message}
    if exc.details:
        payload["details"] = exc.details
    return JSONResponse(status_code=exc.status_code, content=payload)


@app.on_event("startup")
def startup() -> None:
    # Keeps local/dev environments self-contained; prod should still run migrations.
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        ensure_ops_schema(db)
        ensure_provinces_seeded(db)

app.include_router(health.router, tags=["Health"])
app.include_router(portfolios.router)
app.include_router(scores.router)
app.include_router(notifications.router)
app.include_router(exports.router)
app.include_router(climatology.router)
app.include_router(assets.router)
app.include_router(firms.router)
app.include_router(aws_jobs.router)
app.include_router(sentinel_jobs.router)
app.include_router(provinces.router, prefix="/v1/risk", tags=["Risk"])
app.include_router(alerts.router, prefix="/v1/alerts", tags=["Alerts"])
app.include_router(portfolio.router, prefix="/v1/portfolio", tags=["Portfolio"])
app.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])
app.include_router(internal.router, prefix="/internal", tags=["Internal"])
app.include_router(era5_ops.router, prefix="/legacy", tags=["ERA5 Ops"])
