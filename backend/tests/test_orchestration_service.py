from __future__ import annotations

from datetime import date

from app.schemas.orchestration import EnqueueRequest
from app.services import orchestration_service


def test_split_chunks_monthly_for_aws():
    req = EnqueueRequest(
        source="aws_era5",
        job_type="backfill",
        start=date(2024, 1, 15),
        end=date(2024, 3, 3),
        chunking="monthly",
        concurrency=2,
    )
    chunks = orchestration_service.split_chunks(req)
    assert chunks == [
        {"year": 2024, "month": 1},
        {"year": 2024, "month": 2},
        {"year": 2024, "month": 3},
    ]


def test_build_pubsub_messages_deterministic():
    req = EnqueueRequest(
        source="aws_era5",
        job_type="monthly",
        start=date(2024, 2, 1),
        end=date(2024, 3, 1),
        chunking="monthly",
        concurrency=1,
    )
    m1 = orchestration_service.build_pubsub_messages(req, run_id="run-1")
    m2 = orchestration_service.build_pubsub_messages(req, run_id="run-2")
    assert [x["chunk_id"] for x in m1] == [x["chunk_id"] for x in m2]
    assert [x["idempotency_key"] for x in m1] == [x["idempotency_key"] for x in m2]


def test_enqueue_jobs_publish_payload(monkeypatch):
    captured: list[dict] = []

    def _fake_publish(payloads):
        captured.extend(payloads)
        return [f"mid-{idx}" for idx, _ in enumerate(payloads)]

    monkeypatch.setattr(orchestration_service, "publish_json_messages", _fake_publish)
    monkeypatch.setattr(orchestration_service, "_should_skip_publish", lambda *_: False)

    req = EnqueueRequest(
        source="firms",
        job_type="daily",
        start=date(2024, 1, 1),
        end=date(2024, 1, 2),
        chunking="daily",
        concurrency=1,
    )
    out = orchestration_service.enqueue_jobs(req)
    assert out.status == "accepted"
    assert out.published_count == 2
    assert len(captured) == 2
    assert captured[0]["source"] == "firms"
    assert captured[0]["attempt"] == 1
    assert "idempotency_key" in captured[0]

