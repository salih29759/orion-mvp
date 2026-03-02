from __future__ import annotations

from datetime import datetime, timezone

from app.routers import health
from app.routers import jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_health_shape(api_client):
    res = api_client.get("/health")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"status", "version"}
    assert body["status"] == "ok"


def test_health_metrics_shape(api_client, monkeypatch):
    monkeypatch.setattr(health, "get_metrics_payload", lambda: {
        "jobs_last_24h": 0,
        "success_rate": 1.0,
        "avg_duration_seconds": 0.0,
        "bytes_downloaded_last_24h": 0,
    })
    res = api_client.get("/health/metrics")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"jobs_last_24h", "success_rate", "avg_duration_seconds", "bytes_downloaded_last_24h"}


def test_jobs_backfill_and_status_shape(api_client, monkeypatch):
    def fake_create_backfill_job(*, start_month, end_month, bbox, variables, mode, concurrency):
        return {
            "job_id": "job-backfill-1",
            "status": "queued",
            "type": "era5_backfill",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "progress": {"months_total": 3, "months_success": 0, "months_failed": 0},
            "children": [],
        }

    def fake_get_job_status_payload(job_id: str):
        return {
            "job_id": job_id,
            "status": "running",
            "type": "era5_backfill",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "progress": {"months_total": 3, "months_success": 1, "months_failed": 0},
            "children": [],
        }

    monkeypatch.setattr(jobs, "create_backfill_job", fake_create_backfill_job)
    monkeypatch.setattr(jobs, "get_job_status_payload", fake_get_job_status_payload)

    backfill = api_client.post(
        "/jobs/era5/backfill",
        headers=_auth_headers(),
        json={
            "start_month": "2015-01",
            "end_month": "2015-03",
            "bbox": {"north": 42, "west": 26, "south": 36, "east": 45},
            "variables": [
                "2m_temperature",
                "total_precipitation",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "volumetric_soil_water_layer_1",
            ],
            "mode": "monthly",
            "concurrency": 2,
        },
    )
    assert backfill.status_code == 202
    body = backfill.json()
    assert {"job_id", "status", "type", "created_at", "updated_at", "progress", "children"} <= set(body.keys())

    status_res = api_client.get("/jobs/job-backfill-1", headers=_auth_headers())
    assert status_res.status_code == 200
    body2 = status_res.json()
    assert {"job_id", "status", "type", "created_at", "updated_at", "progress", "children"} <= set(body2.keys())
