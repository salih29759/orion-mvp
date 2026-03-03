from app.routers import jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_dem_process_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "create_dem_process",
        lambda **kwargs: {
            "run_id": "dem_run_1",
            "status": "queued",
            "type": "dem_reference_build",
            "created_at": "2026-03-03T00:00:00Z",
            "progress": {
                "tiles_total": 0,
                "tiles_glo30": 0,
                "tiles_glo90": 0,
                "provinces_total": 0,
                "provinces_done": 0,
                "grid_cells_total": 0,
                "grid_cells_done": 0,
                "warning_count": 0,
            },
        },
    )

    res = api_client.post(
        "/jobs/dem/process",
        headers=_auth_headers(),
        json={"grid": True},
    )
    assert res.status_code == 202
    body = res.json()
    assert body["run_id"] == "dem_run_1"
    assert body["type"] == "dem_reference_build"
    assert body["status"] == "queued"


def test_dem_status_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "get_dem_status",
        lambda: {
            "run_id": "dem_run_1",
            "status": "running",
            "type": "dem_reference_build",
            "include_grid": True,
            "created_at": "2026-03-03T00:00:00Z",
            "started_at": "2026-03-03T00:01:00Z",
            "finished_at": None,
            "updated_at": "2026-03-03T00:02:00Z",
            "progress": {
                "tiles_total": 12,
                "tiles_glo30": 10,
                "tiles_glo90": 2,
                "provinces_total": 81,
                "provinces_done": 7,
                "grid_cells_total": 11400,
                "grid_cells_done": 0,
                "warning_count": 0,
            },
            "province_gcs_uri": None,
            "grid_gcs_uri": None,
            "error": None,
        },
    )

    res = api_client.get("/jobs/dem/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["run_id"] == "dem_run_1"
    assert body["status"] == "running"
    assert body["include_grid"] is True
