from __future__ import annotations

from datetime import datetime, timezone

from app.routers import earthquake_jobs
from app.config import settings


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def _payload(job_id: str = "eq_123") -> dict:
    return {
        "job_id": job_id,
        "status": "queued",
        "type": "earthquakes_backfill",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": None,
        "progress": {
            "days_total": 2,
            "days_done": 0,
            "days_failed": 0,
            "failed_days": [],
        },
        "children": [],
    }


def test_earthquake_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(earthquake_jobs, "create_backfill", lambda **kwargs: _payload("eq_backfill_1"))

    res = api_client.post(
        "/jobs/earthquakes/backfill",
        headers=_auth_headers(),
        json={"start": "2026-02-01", "end": "2026-02-02", "min_magnitude": 2.5},
    )
    assert res.status_code == 202
    body = res.json()
    assert body["job_id"] == "eq_backfill_1"
    assert body["type"] == "earthquakes_backfill"
    assert body["status"] in {"queued", "running", "success", "failed", "success_with_warnings", "fail_dq"}


def test_earthquake_status_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(earthquake_jobs, "get_status", lambda: _payload("eq_status_1"))

    res = api_client.get("/jobs/earthquakes/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["job_id"] == "eq_status_1"
    assert {"job_id", "status", "type", "created_at", "updated_at", "progress", "children"} <= set(body.keys())


def test_earthquake_cron_rejects_missing_or_invalid_secret(api_client, monkeypatch):
    monkeypatch.setattr(settings, "cron_secret", "test-secret")
    monkeypatch.setattr(earthquake_jobs, "run_daily", lambda **kwargs: {"status": "accepted"})

    missing = api_client.post("/cron/earthquakes/daily", headers=_auth_headers())
    assert missing.status_code == 401

    bad = api_client.post(
        "/cron/earthquakes/daily",
        headers={**_auth_headers(), "X-Cron-Secret": "wrong"},
    )
    assert bad.status_code == 401
