from __future__ import annotations


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_portfolios_list_contract_shape(api_client):
    res = api_client.get("/portfolios", headers=_auth_headers())
    assert res.status_code == 200
    data = res.json()
    assert isinstance(data, list)
    assert any(p["portfolio_id"] == "demo-3-assets" for p in data)
    assert {"portfolio_id", "name"} <= set(data[0].keys())


def test_portfolio_risk_summary_contract_shape(api_client):
    summary = api_client.get(
        "/portfolios/demo-3-assets/risk-summary?start=2024-01-01&end=2024-01-01",
        headers=_auth_headers(),
    )
    assert summary.status_code == 200
    body = summary.json()
    assert set(body.keys()) == {"portfolio_id", "period", "bands", "peril_averages", "top_assets", "trend"}
    assert set(body["period"].keys()) == {"start", "end"}
    assert len(body["top_assets"]) == 3
    assert {"asset_id", "name", "lat", "lon", "band", "scores"} <= set(body["top_assets"][0].keys())
    assert {"date", "scores"} <= set(body["trend"][0].keys())
    assert "all" in body["trend"][0]["scores"]


def test_portfolio_risk_summary_invalid_date_order(api_client):
    res = api_client.get(
        "/portfolios/demo-3-assets/risk-summary?start=2024-01-10&end=2024-01-01",
        headers=_auth_headers(),
    )
    assert res.status_code == 422

