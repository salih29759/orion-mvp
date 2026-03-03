from app.routers import noaa_jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_noaa_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        noaa_jobs,
        "create_backfill",
        lambda **kwargs: {
            "run_id": "noaa_run_1",
            "status": "queued",
            "type": "noaa_gsod_backfill",
            "created_at": "2026-03-03T00:00:00Z",
            "total_months": 1,
            "effective_start": "2024-01-01",
            "effective_end": "2024-01-31",
            "progress": {
                "months_total": 1,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "pending": 1,
                "rows_written": 0,
                "stations_total": 0,
                "stations_success": 0,
                "stations_failed": 0,
                "strong_wind_proxy_used": 0,
            },
        },
    )

    res = api_client.post(
        "/jobs/noaa/backfill",
        headers=_auth_headers(),
        json={"start": "2024-01-01", "end": "2024-01-31", "concurrency": 2, "force": False},
    )
    assert res.status_code == 202
    body = res.json()
    assert body["run_id"] == "noaa_run_1"
    assert body["type"] == "noaa_gsod_backfill"


def test_noaa_status_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        noaa_jobs,
        "get_status",
        lambda: {
            "run_id": "noaa_run_1",
            "total_months": 1,
            "completed": 1,
            "failed": 0,
            "running": 0,
            "pending": 0,
            "percent_done": 100.0,
            "rows_written": 31,
            "stations_total": 200,
            "stations_success": 200,
            "stations_failed": 0,
            "strong_wind_proxy_used": 5,
            "last_updated": "2026-03-03T00:10:00Z",
            "recent_errors": [],
            "status": "completed",
        },
    )

    res = api_client.get("/jobs/noaa/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "noaa_run_1"
    assert body["status"] == "completed"


def test_noaa_cron_accepts_cron_secret(api_client, monkeypatch):
    monkeypatch.setattr(noaa_jobs.settings, "cron_secret", "cron-123")
    monkeypatch.setattr(noaa_jobs, "run_daily", lambda: {"status": "accepted", "run_id": "noaa_run_2"})

    res = api_client.post(
        "/cron/noaa/daily",
        headers={"X-Cron-Secret": "cron-123"},
    )
    assert res.status_code == 200
    assert res.json()["status"] == "accepted"


def test_noaa_cron_accepts_bearer(api_client, monkeypatch):
    monkeypatch.setattr(noaa_jobs.settings, "cron_secret", "cron-123")
    monkeypatch.setattr(noaa_jobs, "run_daily", lambda: {"status": "accepted", "run_id": "noaa_run_3"})

    res = api_client.post("/cron/noaa/daily", headers=_auth_headers())
    assert res.status_code == 200
    assert res.json()["status"] == "accepted"


def test_noaa_cron_rejects_unauthorized(api_client, monkeypatch):
    monkeypatch.setattr(noaa_jobs.settings, "cron_secret", "cron-123")

    res = api_client.post("/cron/noaa/daily")
    assert res.status_code == 401
    body = res.json()
    assert body["error_code"] == "UNAUTHORIZED"
