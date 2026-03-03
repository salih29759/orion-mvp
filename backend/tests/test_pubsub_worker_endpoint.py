from __future__ import annotations

import base64
import json

from app.routers import pubsub_worker


def _envelope(payload: dict) -> dict:
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return {"message": {"data": encoded}}


def test_pubsub_worker_rejects_missing_auth(api_client):
    res = api_client.post(
        "/internal/pubsub/worker",
        json=_envelope(
            {
                "source": "aws_era5",
                "job_type": "monthly",
                "chunk": {"year": 2024, "month": 1},
                "run_id": "run-1",
                "attempt": 1,
                "chunk_id": "aws_era5:monthly:2024-01",
                "idempotency_key": "abc",
                "concurrency": 1,
            }
        ),
    )
    assert res.status_code == 401


def test_pubsub_worker_rejects_invalid_authorization_header(api_client):
    res = api_client.post(
        "/internal/pubsub/worker",
        headers={"Authorization": "Basic abc"},
        json=_envelope(
            {
                "source": "aws_era5",
                "job_type": "monthly",
                "chunk": {"year": 2024, "month": 1},
                "run_id": "run-1",
                "attempt": 1,
                "chunk_id": "aws_era5:monthly:2024-01",
                "idempotency_key": "abc",
                "concurrency": 1,
            }
        ),
    )
    assert res.status_code == 401


def test_pubsub_worker_accepts_mocked_valid_token(api_client, monkeypatch):
    monkeypatch.setattr(pubsub_worker, "verify_pubsub_oidc_token", lambda _token: {"email": "svc@example.iam.gserviceaccount.com"})
    monkeypatch.setattr(pubsub_worker, "process_pubsub_job_message", lambda message: {"status": "success", "source": message.source})

    res = api_client.post(
        "/internal/pubsub/worker",
        headers={"Authorization": "Bearer token"},
        json=_envelope(
            {
                "source": "aws_era5",
                "job_type": "monthly",
                "chunk": {"year": 2024, "month": 1},
                "run_id": "run-1",
                "attempt": 1,
                "chunk_id": "aws_era5:monthly:2024-01",
                "idempotency_key": "abc",
                "concurrency": 1,
            }
        ),
    )
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "acknowledged"
    assert body["result"]["status"] == "success"


def test_pubsub_worker_nasa_message_safe_ack(api_client, monkeypatch):
    monkeypatch.setattr(pubsub_worker, "verify_pubsub_oidc_token", lambda _token: {"email": "svc@example.iam.gserviceaccount.com"})

    res = api_client.post(
        "/internal/pubsub/worker",
        headers={"Authorization": "Bearer token"},
        json=_envelope(
            {
                "source": "nasa_modis",
                "job_type": "monthly",
                "chunk": {"year": 2024, "month": 1},
                "run_id": "run-1",
                "attempt": 1,
                "chunk_id": "nasa_modis:monthly:2024-01",
                "idempotency_key": "abc",
                "concurrency": 1,
            }
        ),
    )
    assert res.status_code == 200
    body = res.json()
    assert body["result"]["status"] == "skipped"
    assert body["result"]["reason"] == "disabled_in_v1_heavy_source"
