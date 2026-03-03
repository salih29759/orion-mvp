from app.routers import jobs, openaq


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_openaq_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "create_openaq_backfill_job",
        lambda **kwargs: {
            "job_id": "openaq-job-1",
            "status": "queued",
            "type": "openaq_ingest",
            "created_at": "2026-03-02T00:00:00Z",
            "updated_at": None,
            "progress": {"months_total": 2, "months_completed": 0, "months_failed": 0},
            "children": [],
        },
    )

    res = api_client.post(
        "/jobs/openaq/backfill",
        headers=_auth_headers(),
        json={"start": "2026-01-01", "end": "2026-02-28", "concurrency": 5},
    )
    assert res.status_code == 202
    body = res.json()
    assert body["job_id"] == "openaq-job-1"
    assert body["status"] == "queued"
    assert body["type"] == "openaq_ingest"


def test_openaq_status_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "get_openaq_status_payload",
        lambda: {
            "run_id": "openaq-job-1",
            "status": "running",
            "total_months": 12,
            "completed": 4,
            "failed": 1,
            "running": 1,
            "pending": 6,
            "rows_written": 320,
            "requested_start": "2025-01-01",
            "requested_end": "2025-12-31",
            "effective_end": "2025-12-31",
            "stations_total": 220,
            "stations_processed": 118,
            "metadata_gcs_uri": "gs://bucket/metadata/openaq_turkey_stations.json",
            "last_updated": "2026-03-02T12:00:00Z",
            "recent_errors": [],
            "warnings": {"skipped_non_ugm3": 3, "skipped_flagged_total": 2},
        },
    )

    res = api_client.get("/jobs/openaq/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "openaq-job-1"
    assert body["status"] == "running"
    assert body["stations_total"] == 220


def test_openaq_daily_cron_secret_validation(api_client, monkeypatch):
    monkeypatch.setattr(openaq.settings, "cron_secret", "secret-123")

    unauthorized = api_client.post("/cron/openaq/daily", headers={"x-cron-secret": "bad-secret"})
    assert unauthorized.status_code == 401
    assert unauthorized.json()["error_code"] == "UNAUTHORIZED"

    monkeypatch.setattr(
        openaq,
        "run_openaq_daily_update",
        lambda: {
            "status": "accepted",
            "job_id": "openaq-job-2",
            "deduplicated": False,
            "start": "2026-03-01",
            "end": "2026-03-01",
            "months_total": 1,
        },
    )
    authorized = api_client.post("/cron/openaq/daily", headers={"x-cron-secret": "secret-123"})
    assert authorized.status_code == 200
    assert authorized.json()["status"] == "accepted"
