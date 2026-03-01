from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import Base, engine
from app import orm  # noqa: F401
from app.routers import alerts, health, portfolio, provinces

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
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.allowed_origins.split(",") if origin.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup() -> None:
    # Keeps local/dev environments self-contained; prod should still run migrations.
    Base.metadata.create_all(bind=engine)

app.include_router(health.router, tags=["Health"])
app.include_router(provinces.router, prefix="/v1/risk", tags=["Risk"])
app.include_router(alerts.router, prefix="/v1/alerts", tags=["Alerts"])
app.include_router(portfolio.router, prefix="/v1/portfolio", tags=["Portfolio"])
