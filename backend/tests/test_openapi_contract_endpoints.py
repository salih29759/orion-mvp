from __future__ import annotations

from datetime import date, datetime, timezone
import json

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.orm import AssetRiskScoreORM, NotificationORM, PortfolioAssetORM
from app.routers import era5_ops
from main import app
from pipeline import firms_ingestion, risk_scoring


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def _seed_portfolio_and_notifications(SessionLocal) -> None:
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
    db_path = tmp_path / "contract_test.db"
    engine = create_engine(
        f"sqlite:///{db_path}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)

    monkeypatch.setattr(era5_ops, "SessionLocal", SessionLocal)
    monkeypatch.setattr(risk_scoring, "SessionLocal", SessionLocal)
    monkeypatch.setattr(firms_ingestion, "SessionLocal", SessionLocal)

    _seed_portfolio_and_notifications(SessionLocal)

    startup_handlers = list(app.router.on_startup)
    app.router.on_startup.clear()
    try:
        with TestClient(app) as client:
            yield client
    finally:
        app.router.on_startup[:] = startup_handlers


def test_portfolios_and_risk_summary_contract_shape(api_client):
    res = api_client.get("/portfolios", headers=_auth_headers())
    assert res.status_code == 200
    data = res.json()
    assert isinstance(data, list)
    assert any(p["portfolio_id"] == "demo-3-assets" for p in data)
    assert {"portfolio_id", "name"} <= set(data[0].keys())

    summary = api_client.get(
        "/portfolios/demo-3-assets/risk-summary?start=2024-01-01&end=2024-01-01",
        headers=_auth_headers(),
    )
    assert summary.status_code == 200
    body = summary.json()
    assert set(body.keys()) == {"portfolio_id", "period", "bands", "peril_averages", "top_assets", "trend"}
    assert set(body["period"].keys()) == {"start", "end"}
    assert len(body["top_assets"]) == 3
    assert {"asset_id", "name", "lat", "lon", "band", "scores"} <= set(body["top_assets"][0].keys())
    assert {"date", "scores"} <= set(body["trend"][0].keys())
    assert "all" in body["trend"][0]["scores"]


def test_risk_summary_rejects_invalid_date_order(api_client):
    res = api_client.get(
        "/portfolios/demo-3-assets/risk-summary?start=2024-01-10&end=2024-01-01",
        headers=_auth_headers(),
    )
    assert res.status_code == 422


def test_scores_batch_contract_shape_and_deterministic(api_client, monkeypatch):
    def fake_batch_score_assets(*, assets, start_date, end_date, climatology_version, persist=True, include_perils=None):
        include = include_perils or ["heat", "rain", "wind", "drought"]
        out = {}
        for asset in assets:
            rows = []
            for peril in include:
                if peril == "all":
                    continue
                rows.append(
                    {
                        "date": start_date.isoformat(),
                        "peril": peril,
                        "score_0_100": 42,
                        "band": "Moderate",
                        "exposure": {"value": 42},
                        "drivers": [f"{peril} driver"],
                    }
                )
            out[asset["asset_id"]] = rows
        return {"run_id": "run-fixed", "assets": out}

    monkeypatch.setattr(era5_ops, "batch_score_assets", fake_batch_score_assets)

    payload = {
        "assets": [
            {"asset_id": "a1", "lat": 41.01, "lon": 28.97},
            {"asset_id": "a2", "lat": 39.93, "lon": 32.85},
        ],
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "climatology_version": "v1_baseline_2015_2024",
        "include_perils": ["heat", "rain", "wind", "drought"],
    }
    r1 = api_client.post("/scores/batch", headers=_auth_headers(), json=payload)
    r2 = api_client.post("/scores/batch", headers=_auth_headers(), json=payload)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json() == r2.json()

    body = r1.json()
    assert set(body.keys()) == {"run_id", "climatology_version", "results"}
    assert {"asset_id", "series"} <= set(body["results"][0].keys())
    assert {"date", "scores", "bands", "drivers"} <= set(body["results"][0]["series"][0].keys())
    assert body["results"][0]["series"][0]["scores"]["all"] == 42


def test_notifications_list_and_ack_contract_shape(api_client):
    res = api_client.get("/notifications?portfolio_id=demo-3-assets", headers=_auth_headers())
    assert res.status_code == 200
    rows = res.json()
    assert len(rows) == 1
    row = rows[0]
    assert set(row.keys()) == {
        "id",
        "severity",
        "type",
        "portfolio_id",
        "asset_id",
        "created_at",
        "acknowledged_at",
        "payload",
    }
    assert row["severity"] in {"low", "medium", "high"}
    assert row["acknowledged_at"] is None

    ack = api_client.post("/notifications/ntf-1/ack", headers=_auth_headers())
    assert ack.status_code == 200
    ack_body = ack.json()
    assert set(ack_body.keys()) == {"id", "acknowledged_at"}
    assert ack_body["id"] == "ntf-1"
    assert isinstance(ack_body["acknowledged_at"], str)


def test_export_portfolio_contract_status_and_path(api_client, monkeypatch):
    class _DummyBlob:
        def upload_from_string(self, *args, **kwargs):
            return None

    class _DummyBucket:
        def blob(self, *args, **kwargs):
            return _DummyBlob()

    class _DummyStorageClient:
        def bucket(self, *args, **kwargs):
            return _DummyBucket()

    def fake_batch_score_assets(*, assets, start_date, end_date, climatology_version, persist=True, include_perils=None):
        out = {}
        for asset in assets:
            out[asset["asset_id"]] = [
                {
                    "date": start_date.isoformat(),
                    "peril": "heat",
                    "score_0_100": 40,
                    "band": "Moderate",
                    "exposure": {"value": 40},
                    "drivers": ["heat driver"],
                }
            ]
        return {"run_id": "run-export", "assets": out}

    monkeypatch.setattr(era5_ops, "batch_score_assets", fake_batch_score_assets)
    monkeypatch.setattr(era5_ops.storage, "Client", _DummyStorageClient)
    monkeypatch.setattr(era5_ops, "_generate_signed_url", lambda *args, **kwargs: None)
    monkeypatch.setattr(era5_ops, "save_export_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(era5_ops.settings, "era5_gcs_bucket", "test-bucket")

    res = api_client.post(
        "/export/portfolio",
        headers=_auth_headers(),
        json={
            "portfolio_id": "demo-3-assets",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "format": "csv",
            "include_drivers": True,
            "assets": [{"asset_id": "a1", "lat": 41.01, "lon": 28.97}],
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"export_id", "status", "path", "download_url"}
    assert body["status"] in {"queued", "running", "success", "failed"}
    assert body["path"].startswith("gs://test-bucket/exports/demo-3-assets/")

