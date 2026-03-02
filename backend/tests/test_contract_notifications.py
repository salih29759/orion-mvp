from __future__ import annotations


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_notifications_list_and_ack_contract_shape(api_client):
    res = api_client.get("/notifications?portfolio_id=demo-3-assets", headers=_auth_headers())
    assert res.status_code == 200
    rows = res.json()
    assert len(rows) == 1
    row = rows[0]
    assert set(row.keys()) == {
        "id",
        "severity",
        "type",
        "portfolio_id",
        "asset_id",
        "created_at",
        "acknowledged_at",
        "payload",
    }
    assert row["severity"] in {"low", "medium", "high"}
    assert row["acknowledged_at"] is None

    ack = api_client.post("/notifications/ntf-1/ack", headers=_auth_headers())
    assert ack.status_code == 200
    ack_body = ack.json()
    assert set(ack_body.keys()) == {"id", "acknowledged_at"}
    assert ack_body["id"] == "ntf-1"
    assert isinstance(ack_body["acknowledged_at"], str)

