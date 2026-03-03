from app.routers import cams_jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_cams_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        cams_jobs,
        "create_backfill",
        lambda **kwargs: {
            "run_id": "cams_123",
            "status": "queued",
            "type": "cams_backfill",
            "created_at": "2026-03-02T00:00:00Z",
            "progress": {
                "requested_start": "2019-01-01",
                "requested_end": "2019-01-01",
                "effective_start": "2019-01-01",
                "effective_end": "2019-01-01",
                "months_total": 1,
                "completed": 0,
                "failed": 0,
                "running": 0,
                "pending": 1,
            },
        },
    )

    res = api_client.post(
        "/jobs/cams/backfill",
        headers=_auth_headers(),
        json={"start": "2019-01-01", "end": "2019-01-31", "concurrency": 1, "force": False},
    )
    assert res.status_code == 202
    body = res.json()
    assert body["run_id"] == "cams_123"
    assert body["status"] == "queued"
    assert body["type"] == "cams_backfill"
    assert body["progress"]["months_total"] == 1


def test_cams_backfill_status_endpoint_latest(api_client, monkeypatch):
    monkeypatch.setattr(
        cams_jobs,
        "get_latest_status",
        lambda run_id=None: {
            "run_id": "cams_123",
            "total_months": 2,
            "completed": 1,
            "failed": 0,
            "running": 1,
            "pending": 0,
            "percent_done": 50.0,
            "last_updated": "2026-03-02T00:10:00Z",
            "recent_errors": [],
            "status": "running",
        },
    )

    res = api_client.get("/jobs/cams/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "cams_123"
    assert body["total_months"] == 2
    assert body["percent_done"] == 50.0


def test_cams_backfill_status_endpoint_by_run_id(api_client, monkeypatch):
    seen: dict[str, str | None] = {"run_id": None}

    def _mock(run_id=None):
        seen["run_id"] = run_id
        return {
            "run_id": run_id,
            "total_months": 1,
            "completed": 1,
            "failed": 0,
            "running": 0,
            "pending": 0,
            "percent_done": 100.0,
            "last_updated": "2026-03-02T00:11:00Z",
            "recent_errors": [],
            "status": "completed",
        }

    monkeypatch.setattr(cams_jobs, "get_latest_status", _mock)

    res = api_client.get("/jobs/cams/status?run_id=cams_run_x", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "cams_run_x"
    assert seen["run_id"] == "cams_run_x"
