from __future__ import annotations

from app.routers import exports


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_export_portfolio_contract_status_and_path(api_client, monkeypatch):
    def fake_create_export(*, portfolio_id, start_date, end_date, include_drivers):
        return {
            "export_id": "exp-1",
            "status": "success",
            "path": f"gs://test-bucket/exports/{portfolio_id}/exp-1.csv",
            "download_url": None,
        }

    monkeypatch.setattr(exports, "create_portfolio_export", fake_create_export)

    res = api_client.post(
        "/export/portfolio",
        headers=_auth_headers(),
        json={
            "portfolio_id": "demo-3-assets",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "format": "csv",
            "include_drivers": True,
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"export_id", "status", "path", "download_url"}
    assert body["status"] in {"queued", "running", "success", "failed"}
    assert body["path"].startswith("gs://test-bucket/exports/demo-3-assets/")

