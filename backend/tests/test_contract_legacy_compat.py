from __future__ import annotations

from app.routers import alerts, provinces


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_legacy_risk_provinces_still_works(api_client, monkeypatch):
    monkeypatch.setattr(provinces, "list_latest_province_scores", lambda *args, **kwargs: [])
    monkeypatch.setattr(provinces, "get_latest_as_of_date", lambda *args, **kwargs: None)
    res = api_client.get("/v1/risk/provinces?limit=1", headers=_auth_headers())
    assert res.status_code == 200


def test_legacy_alerts_still_works(api_client, monkeypatch):
    monkeypatch.setattr(alerts, "repo_list_active_alerts", lambda *args, **kwargs: [])
    res = api_client.get("/v1/alerts/active?limit=1", headers=_auth_headers())
    assert res.status_code == 200
