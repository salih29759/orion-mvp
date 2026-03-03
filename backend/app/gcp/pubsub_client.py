from __future__ import annotations

import json
from typing import Any

from google.cloud import pubsub_v1

from app.config import settings
from app.errors import ApiError


def _publisher_client() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()


def _topic_path(client: pubsub_v1.PublisherClient) -> str:
    project_id = settings.pubsub_project_id
    if not project_id:
        raise ApiError(
            status_code=503,
            error_code="CONFIG_ERROR",
            message="PUBSUB_PROJECT_ID is not configured",
        )
    if not settings.pubsub_topic:
        raise ApiError(
            status_code=503,
            error_code="CONFIG_ERROR",
            message="PUBSUB_TOPIC is not configured",
        )
    return client.topic_path(project_id, settings.pubsub_topic)


def publish_json_messages(payloads: list[dict[str, Any]]) -> list[str]:
    if not payloads:
        return []
    client = _publisher_client()
    topic_path = _topic_path(client)
    futures = []
    for payload in payloads:
        data = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        futures.append(client.publish(topic_path, data=data))
    return [f.result() for f in futures]

