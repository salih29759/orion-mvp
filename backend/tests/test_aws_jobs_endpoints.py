from app.routers import aws_jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_aws_catalog_sync_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        aws_jobs,
        "create_catalog_sync",
        lambda prefixes, max_keys_per_prefix: {
            "job_id": "aws-cat-1",
            "status": "success",
            "type": "aws_era5_catalog_sync",
            "created_at": "2026-03-02T00:00:00Z",
            "updated_at": "2026-03-02T00:00:01Z",
            "progress": {"objects_scanned": 123},
            "children": [],
        },
    )

    res = api_client.post(
        "/jobs/aws-era5/catalog/sync",
        headers=_auth_headers(),
        json={"max_keys_per_prefix": 100},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["type"] == "aws_era5_catalog_sync"
    assert body["status"] in {"success", "running", "queued", "failed"}


def test_aws_catalog_latest_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        aws_jobs,
        "get_catalog_latest",
        lambda: {
            "bucket": "nsf-ncar-era5",
            "region": "us-west-2",
            "latest_common_month": "2025-10",
            "latest_by_variable": {
                "2m_temperature": "2025-10",
                "total_precipitation": "2025-10",
            },
        },
    )
    res = api_client.get("/jobs/aws-era5/catalog/latest", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["bucket"] == "nsf-ncar-era5"
    assert "latest_by_variable" in body


def test_aws_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        aws_jobs,
        "create_backfill",
        lambda **kwargs: {
            "job_id": "aws-bf-1",
            "status": "queued",
            "type": "aws_era5_backfill",
            "created_at": "2026-03-02T00:00:00Z",
            "updated_at": None,
            "progress": {"months_total": 1, "months_success": 0, "months_failed": 0},
            "children": [],
        },
    )
    res = api_client.post(
        "/jobs/aws-era5/backfill",
        headers=_auth_headers(),
        json={
            "start": "2024-01-01",
            "end": "2024-01-31",
            "mode": "points",
            "points_set": "assets+provinces",
            "bbox": {"north": 42, "west": 26, "south": 36, "east": 45},
            "variables": [
                "2m_temperature",
                "total_precipitation",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "volumetric_soil_water_layer_1",
            ],
            "concurrency": 2,
            "force": False,
        },
    )
    assert res.status_code == 202
    body = res.json()
    assert body["type"] == "aws_era5_backfill"
    assert body["status"] == "queued"
