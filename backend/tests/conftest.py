from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
import types

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from app.database import Base
from app.orm import AssetRiskScoreORM, NotificationORM, PortfolioAssetORM


# Lightweight stubs so unit tests can run without cloud/CDS client libs.
if "cdsapi" not in sys.modules:
    cdsapi = types.ModuleType("cdsapi")

    class _DummyClient:  # noqa: D401
        def __init__(self, *args, **kwargs):
            pass

        def retrieve(self, *args, **kwargs):
            raise RuntimeError("cdsapi stub should not be called in unit tests")

    cdsapi.Client = _DummyClient
    sys.modules["cdsapi"] = cdsapi

if "google.cloud" not in sys.modules:
    google = types.ModuleType("google")
    auth = types.ModuleType("google.auth")
    auth_transport = types.ModuleType("google.auth.transport")
    auth_transport_requests = types.ModuleType("google.auth.transport.requests")
    oauth2 = types.ModuleType("google.oauth2")
    oauth2_id_token = types.ModuleType("google.oauth2.id_token")
    cloud = types.ModuleType("google.cloud")
    pubsub_v1 = types.ModuleType("google.cloud.pubsub_v1")
    storage = types.ModuleType("google.cloud.storage")

    class _DummyBlob:
        def upload_from_filename(self, *args, **kwargs):
            pass

        def download_to_filename(self, *args, **kwargs):
            raise RuntimeError("storage stub should not be called in unit tests")

        def upload_from_string(self, *args, **kwargs):
            pass

        def generate_signed_url(self, *args, **kwargs):
            return None

    class _DummyBucket:
        def blob(self, *args, **kwargs):
            return _DummyBlob()

    class _DummyStorageClient:
        def bucket(self, *args, **kwargs):
            return _DummyBucket()

    class _DummyFuture:
        def result(self):
            return "message-id"

    class _DummyPublisherClient:
        def topic_path(self, project_id, topic):
            return f"projects/{project_id}/topics/{topic}"

        def publish(self, *args, **kwargs):
            return _DummyFuture()

    class _DummyRequest:  # noqa: D401
        pass

    class _DummyCreds:
        token = None
        service_account_email = None

        def refresh(self, *args, **kwargs):
            return None

    def _dummy_google_auth_default(*args, **kwargs):
        return _DummyCreds(), None

    def _dummy_verify_oauth2_token(*args, **kwargs):
        return {
            "aud": kwargs.get("audience"),
            "iss": "https://accounts.google.com",
            "email": "pubsub-push@example.iam.gserviceaccount.com",
        }

    auth.default = _dummy_google_auth_default
    auth_transport_requests.Request = _DummyRequest
    auth.transport = auth_transport
    auth_transport.requests = auth_transport_requests
    oauth2_id_token.verify_oauth2_token = _dummy_verify_oauth2_token
    oauth2.id_token = oauth2_id_token
    pubsub_v1.PublisherClient = _DummyPublisherClient
    storage.Client = _DummyStorageClient
    cloud.pubsub_v1 = pubsub_v1
    cloud.storage = storage
    google.auth = auth
    google.oauth2 = oauth2
    google.cloud = cloud
    sys.modules["google"] = google
    sys.modules["google.auth"] = auth
    sys.modules["google.auth.transport"] = auth_transport
    sys.modules["google.auth.transport.requests"] = auth_transport_requests
    sys.modules["google.oauth2"] = oauth2
    sys.modules["google.oauth2.id_token"] = oauth2_id_token
    sys.modules["google.cloud"] = cloud
    sys.modules["google.cloud.pubsub_v1"] = pubsub_v1
    sys.modules["google.cloud.storage"] = storage


def _seed_contract_fixture(SessionLocal):
    with SessionLocal() as db:
        db.add_all(
            [
                PortfolioAssetORM(portfolio_id="demo-3-assets", asset_id="a1", lat=41.01, lon=28.97),
                PortfolioAssetORM(portfolio_id="demo-3-assets", asset_id="a2", lat=39.93, lon=32.85),
                PortfolioAssetORM(portfolio_id="demo-3-assets", asset_id="a3", lat=38.42, lon=27.14),
            ]
        )
        for asset_id, score in [("a1", 80), ("a2", 50), ("a3", 20)]:
            for peril in ["heat", "rain", "wind", "drought"]:
                db.add(
                    AssetRiskScoreORM(
                        asset_id=asset_id,
                        score_date=date(2024, 1, 1),
                        peril=peril,
                        scenario="historical",
                        horizon="current",
                        likelihood="observed",
                        score_0_100=score,
                        band="Extreme" if score >= 80 else "Moderate" if score >= 40 else "Minor",
                        exposure_json=json.dumps({"value": score}),
                        drivers_json=json.dumps([f"{peril} driver"]),
                        run_id="seed-run",
                        climatology_version="v1_baseline_2015_2024",
                        data_version="era5_daily_v1",
                    )
                )
        db.add(
            NotificationORM(
                id="ntf-1",
                customer_id="cust-1",
                portfolio_id="demo-3-assets",
                asset_id="a1",
                type="wildfire_proximity",
                severity="high",
                payload_json=json.dumps({"nearest_fire_distance_km": 4.2}),
                dedup_key="demo-3-assets:a1:wildfire:high:2026-03-01",
            )
        )
        db.commit()


@pytest.fixture
def api_client(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from app import database as app_database
    from main import app
    from app.routers import era5_ops
    from app.services import export_service, orchestration_service, portfolio_service
    from pipeline import firms_ingestion, risk_scoring

    db_path = tmp_path / "contract_test.db"
    engine = create_engine(
        f"sqlite:///{db_path}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)

    monkeypatch.setattr(app_database, "SessionLocal", TestSession)
    monkeypatch.setattr(portfolio_service, "SessionLocal", TestSession)
    monkeypatch.setattr(export_service, "SessionLocal", TestSession)
    monkeypatch.setattr(era5_ops, "SessionLocal", TestSession)
    monkeypatch.setattr(risk_scoring, "SessionLocal", TestSession)
    monkeypatch.setattr(firms_ingestion, "SessionLocal", TestSession)
    monkeypatch.setattr(orchestration_service, "SessionLocal", TestSession)

    _seed_contract_fixture(TestSession)

    startup_handlers = list(app.router.on_startup)
    app.router.on_startup.clear()
    try:
        with TestClient(app) as client:
            yield client
    finally:
        app.router.on_startup[:] = startup_handlers
