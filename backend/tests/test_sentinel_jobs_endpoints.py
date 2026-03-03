from app.routers import sentinel_jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_sentinel_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        sentinel_jobs,
        "create_backfill",
        lambda **kwargs: {
            "run_id": "sentinel_run_1",
            "total_months": 3,
        },
    )
    res = api_client.post(
        "/jobs/sentinel/backfill",
        headers=_auth_headers(),
        json={
            "start": "2019-01-01",
            "end": "2019-03-31",
            "concurrency": 2,
            "force": False,
        },
    )
    assert res.status_code == 202
    body = res.json()
    assert body["run_id"] == "sentinel_run_1"
    assert body["total_months"] == 3


def test_sentinel_status_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        sentinel_jobs,
        "get_status",
        lambda: {
            "run_id": "sentinel_run_1",
            "total_months": 4,
            "completed": 2,
            "failed": 1,
            "running": 1,
            "percent_done": 75.0,
            "last_updated": "2026-03-03T18:00:00+00:00",
            "failed_months": ["2020-02"],
        },
    )
    res = api_client.get("/jobs/sentinel/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["total_months"] == 4
    assert body["failed_months"] == ["2020-02"]


def test_sentinel_cron_accepts_cron_secret(api_client, monkeypatch):
    monkeypatch.setattr(sentinel_jobs.settings, "cron_secret", "cron-secret-1")
    monkeypatch.setattr(sentinel_jobs.settings, "orion_backend_api_key", None)
    monkeypatch.setattr(
        sentinel_jobs,
        "run_monthly_update",
        lambda: {"run_id": "sentinel_run_2", "total_months": 1},
    )

    res = api_client.post("/cron/sentinel/monthly", headers={"X-Cron-Secret": "cron-secret-1"})
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "sentinel_run_2"
    assert body["total_months"] == 1


def test_sentinel_cron_accepts_orion_bearer(api_client, monkeypatch):
    monkeypatch.setattr(sentinel_jobs.settings, "cron_secret", None)
    monkeypatch.setattr(sentinel_jobs.settings, "orion_backend_api_key", "orion-only-key")
    monkeypatch.setattr(
        sentinel_jobs,
        "run_monthly_update",
        lambda: {"run_id": "sentinel_run_3", "total_months": 1},
    )

    res = api_client.post("/cron/sentinel/monthly", headers={"Authorization": "Bearer orion-only-key"})
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "sentinel_run_3"


def test_sentinel_cron_rejects_non_orion_bearer(api_client, monkeypatch):
    monkeypatch.setattr(sentinel_jobs.settings, "cron_secret", None)
    monkeypatch.setattr(sentinel_jobs.settings, "orion_backend_api_key", "orion-only-key")
    monkeypatch.setattr(
        sentinel_jobs,
        "run_monthly_update",
        lambda: {"run_id": "sentinel_run_4", "total_months": 1},
    )

    res = api_client.post("/cron/sentinel/monthly", headers={"Authorization": "Bearer orion-dev-key-2024"})
    assert res.status_code == 401
    assert res.json()["error_code"] == "UNAUTHORIZED"
