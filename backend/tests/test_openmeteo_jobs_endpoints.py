from __future__ import annotations

from datetime import datetime, timezone

from app.routers import jobs, openmeteo_jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_openmeteo_backfill_endpoint_shape(api_client, monkeypatch):
    monkeypatch.setattr(
        openmeteo_jobs,
        "create_openmeteo_backfill_job",
        lambda start, end, concurrency: {
            "job_id": "om-bf-1",
            "status": "queued",
            "type": "openmeteo_backfill",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "progress": {"years_total": 1, "years_success": 0, "years_failed": 0},
            "children": [],
        },
    )

    res = api_client.post(
        "/jobs/openmeteo/backfill",
        headers=_auth_headers(),
        json={"start": "2025-01-01", "end": "2025-12-31", "concurrency": 10},
    )

    assert res.status_code == 202
    body = res.json()
    assert body["type"] == "openmeteo_backfill"
    assert body["status"] == "queued"
    assert {"job_id", "status", "type", "created_at", "updated_at", "progress", "children"} <= set(body.keys())


def test_openmeteo_forecast_endpoint_shape(api_client, monkeypatch):
    monkeypatch.setattr(
        openmeteo_jobs,
        "create_openmeteo_forecast_job",
        lambda forecast_days: {
            "job_id": "om-fc-1",
            "status": "queued",
            "type": "openmeteo_forecast",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "progress": {"forecast_days": forecast_days},
            "children": [],
        },
    )

    res = api_client.post(
        "/jobs/openmeteo/forecast",
        headers=_auth_headers(),
        json={"forecast_days": 16},
    )

    assert res.status_code == 202
    body = res.json()
    assert body["type"] == "openmeteo_forecast"
    assert body["progress"]["forecast_days"] == 16


def test_openmeteo_daily_cron_requires_bearer_and_secret(api_client, monkeypatch):
    monkeypatch.setattr(openmeteo_jobs.settings, "cron_secret", "cron-secret")
    monkeypatch.setattr(
        openmeteo_jobs,
        "run_openmeteo_daily_update",
        lambda: {
            "status": "accepted",
            "job_id": "om-d-1",
            "deduplicated": False,
            "target_date": "2026-03-01",
        },
    )

    no_auth = api_client.post("/cron/openmeteo/daily")
    assert no_auth.status_code == 401

    no_secret = api_client.post("/cron/openmeteo/daily", headers=_auth_headers())
    assert no_secret.status_code == 401

    wrong_secret = api_client.post(
        "/cron/openmeteo/daily",
        headers={**_auth_headers(), "x-cron-secret": "wrong"},
    )
    assert wrong_secret.status_code == 401

    ok = api_client.post(
        "/cron/openmeteo/daily",
        headers={**_auth_headers(), "x-cron-secret": "cron-secret"},
    )
    assert ok.status_code == 200
    assert ok.json()["status"] == "accepted"


def test_jobs_status_supports_openmeteo_type(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "get_job_status_payload",
        lambda job_id: {
            "job_id": job_id,
            "status": "running",
            "type": "openmeteo_backfill",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "progress": {"years_total": 2, "years_success": 1, "years_failed": 0},
            "children": [],
        },
    )

    res = api_client.get("/jobs/om-bf-1", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["type"] == "openmeteo_backfill"
    assert body["progress"]["years_success"] == 1
