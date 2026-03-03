from __future__ import annotations

from app.services import orchestration_service


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_jobs_enqueue_nasa_disabled(api_client, monkeypatch):
    monkeypatch.setattr(
        orchestration_service,
        "publish_json_messages",
        lambda *_: (_ for _ in ()).throw(AssertionError("publish should not be called for disabled nasa sources")),
    )

    res = api_client.post(
        "/jobs/enqueue",
        headers=_auth_headers(),
        json={
            "source": "nasa_smap",
            "job_type": "backfill",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "chunking": "monthly",
            "concurrency": 1,
        },
    )
    assert res.status_code == 202
    body = res.json()
    assert body["status"] == "accepted"
    assert body["disabled"] is True
    assert body["reason"] == "disabled_in_v1_heavy_source"
    assert body["published_count"] == 0

