from app.routers import jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_glofas_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "create_glofas_backfill_job",
        lambda **kwargs: {
            "job_id": "glofas-bf-1",
            "status": "queued",
            "type": "glofas_backfill",
            "created_at": "2026-03-03T00:00:00Z",
            "updated_at": None,
            "progress": {"months_total": 1, "months_success": 0, "months_failed": 0},
            "children": [],
        },
    )

    res = api_client.post(
        "/jobs/glofas/backfill",
        headers=_auth_headers(),
        json={
            "start": "1979-01-01",
            "end": "1979-01-31",
            "concurrency": 2,
        },
    )
    assert res.status_code == 202
    body = res.json()
    assert body["type"] == "glofas_backfill"
    assert body["status"] == "queued"
    assert {"job_id", "status", "type", "created_at", "updated_at", "progress", "children"} <= set(body.keys())


def test_glofas_status_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "get_glofas_status_payload",
        lambda: {
            "run_id": "glofas-bf-1",
            "status": "running",
            "total_months": 12,
            "completed": 5,
            "failed": 1,
            "running": 2,
            "percent_done": 50.0,
            "failed_months": ["1980-01"],
            "eta_hours": 3.5,
            "last_updated": "2026-03-03T12:00:00Z",
            "baseline_ready": False,
            "baseline_uri": None,
        },
    )

    res = api_client.get("/jobs/glofas/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "glofas-bf-1"
    assert body["status"] == "running"
    assert body["failed_months"] == ["1980-01"]
    assert body["baseline_ready"] is False
