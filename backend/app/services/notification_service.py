from __future__ import annotations

import json

from app.errors import ApiError
from pipeline.firms_ingestion import ack_notification, list_notifications


def get_notifications(portfolio_id: str | None = None) -> list[dict]:
    rows = list_notifications(portfolio_id=portfolio_id)
    out: list[dict] = []
    for row in rows:
        payload = {}
        if row.payload_json:
            try:
                payload = json.loads(row.payload_json)
            except Exception:  # noqa: BLE001
                payload = {"raw": row.payload_json}
        severity = (row.severity or "low").lower()
        if severity not in {"low", "medium", "high"}:
            severity = "low"
        out.append(
            {
                "id": row.id,
                "severity": severity,
                "type": row.type,
                "portfolio_id": row.portfolio_id,
                "asset_id": row.asset_id,
                "created_at": row.created_at,
                "acknowledged_at": row.acknowledged_at,
                "payload": payload,
            }
        )
    return out


def acknowledge_notification(notification_id: str) -> dict:
    row = ack_notification(notification_id)
    if not row or not row.get("acknowledged_at"):
        raise ApiError(status_code=404, error_code="NOT_FOUND", message=f"Notification '{notification_id}' not found")
    return {"id": row["id"], "acknowledged_at": row["acknowledged_at"]}

