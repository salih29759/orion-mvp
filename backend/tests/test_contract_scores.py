from __future__ import annotations

from app.routers import scores


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer orion-dev-key-2024"}


def test_scores_batch_contract_shape_and_deterministic(api_client, monkeypatch):
    def fake_run_batch_scores(*, assets, start_date, end_date, climatology_version, include_perils):
        results = []
        for asset in assets:
            results.append(
                {
                    "asset_id": asset["asset_id"],
                    "series": [
                        {
                            "date": start_date.isoformat(),
                            "scores": {"heat": 42.0, "rain": 42.0, "wind": 42.0, "drought": 42.0, "all": 42.0},
                            "bands": {
                                "heat": "moderate",
                                "rain": "moderate",
                                "wind": "moderate",
                                "drought": "moderate",
                                "all": "moderate",
                            },
                            "drivers": {"heat": ["heat driver"]},
                        }
                    ],
                }
            )
        return {"run_id": "run-fixed", "climatology_version": climatology_version, "results": results}

    monkeypatch.setattr(scores, "run_batch_scores", fake_run_batch_scores)

    payload = {
        "assets": [
            {"asset_id": "a1", "lat": 41.01, "lon": 28.97},
            {"asset_id": "a2", "lat": 39.93, "lon": 32.85},
        ],
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "climatology_version": "v1_baseline_2015_2024",
        "include_perils": ["heat", "rain", "wind", "drought"],
    }
    r1 = api_client.post("/scores/batch", headers=_auth_headers(), json=payload)
    r2 = api_client.post("/scores/batch", headers=_auth_headers(), json=payload)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json() == r2.json()

    body = r1.json()
    assert set(body.keys()) == {"run_id", "climatology_version", "results"}
    assert {"asset_id", "series"} <= set(body["results"][0].keys())
    assert {"date", "scores", "bands", "drivers"} <= set(body["results"][0]["series"][0].keys())
    assert body["results"][0]["series"][0]["scores"]["all"] == 42.0

