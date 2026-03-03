from __future__ import annotations

from datetime import datetime, timezone

from app.routers import jobs


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def _job_payload(job_id: str, job_type: str) -> dict:
    return {
        "job_id": job_id,
        "status": "queued",
        "type": job_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": None,
        "progress": {
            "months_total": 1,
            "months_completed": 0,
            "months_failed": 0,
            "rows_written": 0,
            "files_downloaded": 0,
            "files_written": 0,
        },
        "children": [],
    }


def test_nasa_smap_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "create_nasa_smap_backfill_job",
        lambda **kwargs: _job_payload("nasa-smap-1", "nasa_smap_backfill"),
    )

    res = api_client.post(
        "/jobs/nasa/smap/backfill",
        headers=_auth_headers(),
        json={"start": "2024-01-01", "end": "2024-01-31"},
    )
    assert res.status_code == 202
    body = res.json()
    assert body["job_id"] == "nasa-smap-1"
    assert body["type"] == "nasa_smap_backfill"
    assert body["status"] in {"queued", "running", "success", "failed", "success_with_warnings", "fail_dq"}


def test_nasa_modis_backfill_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "create_nasa_modis_backfill_job",
        lambda **kwargs: _job_payload("nasa-modis-1", "nasa_modis_backfill"),
    )

    res = api_client.post(
        "/jobs/nasa/modis/backfill",
        headers=_auth_headers(),
        json={"start": "2021-07-01", "end": "2021-07-31"},
    )
    assert res.status_code == 202
    body = res.json()
    assert body["job_id"] == "nasa-modis-1"
    assert body["type"] == "nasa_modis_backfill"
    assert body["status"] in {"queued", "running", "success", "failed", "success_with_warnings", "fail_dq"}


def test_nasa_status_endpoint(api_client, monkeypatch):
    monkeypatch.setattr(
        jobs,
        "get_nasa_status_payload",
        lambda: {
            "smap": _job_payload("nasa-smap-2", "nasa_smap_backfill"),
            "modis": _job_payload("nasa-modis-2", "nasa_modis_backfill"),
        },
    )

    res = api_client.get("/jobs/nasa/status", headers=_auth_headers())
    assert res.status_code == 200
    body = res.json()
    assert body["smap"]["type"] == "nasa_smap_backfill"
    assert body["modis"]["type"] == "nasa_modis_backfill"
