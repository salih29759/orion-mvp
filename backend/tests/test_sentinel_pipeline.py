from __future__ import annotations

from datetime import date
import os

import numpy as np
import pandas as pd
import pytest

from app.pipelines.sentinel_hub import sentinel_client, sentinel_job


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def test_request_access_token_success(monkeypatch):
    monkeypatch.setattr(sentinel_client.settings, "sentinel_hub_client_id", "client-id")
    monkeypatch.setattr(sentinel_client.settings, "sentinel_hub_client_secret", "client-secret")
    monkeypatch.setattr(
        sentinel_client.requests,
        "post",
        lambda *args, **kwargs: _FakeResponse(200, {"access_token": "token-abc", "expires_in": 3600}),
    )

    token = sentinel_client.request_access_token()
    assert token == "token-abc"


def test_request_access_token_unauthorized(monkeypatch):
    monkeypatch.setattr(sentinel_client.settings, "sentinel_hub_client_id", "client-id")
    monkeypatch.setattr(sentinel_client.settings, "sentinel_hub_client_secret", "client-secret")
    monkeypatch.setattr(
        sentinel_client.requests,
        "post",
        lambda *args, **kwargs: _FakeResponse(401, {"error": "invalid_client"}),
    )

    with pytest.raises(RuntimeError, match="401/403"):
        sentinel_client.request_access_token()


def test_aggregate_metrics_with_datamask():
    raster = np.array(
        [
            [[0.2, 0.1, 2.0, 1.0], [0.5, 0.3, 3.0, 1.0]],
            [[-0.3, -0.2, 1.0, 0.0], [0.7, 0.2, 4.0, 1.0]],
        ],
        dtype=np.float32,
    )
    out = sentinel_job.aggregate_metrics(raster)
    assert out["ndvi_mean"] is not None
    assert -1.0 <= float(out["ndvi_mean"]) <= 1.0
    assert out["ndvi_min"] == pytest.approx(0.2, rel=1e-6)
    assert out["nbr_mean"] == pytest.approx((0.1 + 0.3 + 0.2) / 3.0, rel=1e-6)
    assert out["bai_max"] == pytest.approx(4.0, rel=1e-6)
    assert out["cloud_coverage_pct"] == pytest.approx(25.0, rel=1e-6)


def test_month_output_contract_rows_and_ndvi_range(monkeypatch):
    provinces = pd.DataFrame(
        [
            {"province_id": f"province:{i:02d}", "lat": 36.0 + (i * 0.01), "lon": 26.0 + (i * 0.01)}
            for i in range(1, 82)
        ]
    )
    monkeypatch.setattr(sentinel_job, "load_province_centroids", lambda: provinces)
    monkeypatch.setattr(sentinel_job.settings, "sentinel_bbox_half_size_deg", 0.125)

    def _fake_fetcher(**kwargs):
        return np.array(
            [
                [[0.2, 0.1, 2.0, 1.0], [0.4, 0.2, 3.0, 1.0]],
                [[0.1, 0.0, 1.0, 1.0], [0.3, 0.1, 4.0, 1.0]],
            ],
            dtype=np.float32,
        )

    frame = sentinel_job.build_month_rows(
        month_value=date(2020, 6, 1),
        run_id="sentinel_test_run",
        raster_fetcher=_fake_fetcher,
    )
    assert len(frame.index) == 81
    assert frame["province_id"].iloc[0] == "province:01"
    assert frame["ndvi_mean"].between(-1.0, 1.0).all()


@pytest.mark.skipif(os.getenv("RUN_SENTINEL_INTEGRATION") != "1", reason="Set RUN_SENTINEL_INTEGRATION=1 to run")
def test_integration_one_province_one_month_request_executes():
    if sentinel_client.SHConfig is None:
        pytest.skip("sentinelhub package not installed")
    if not sentinel_client.settings.sentinel_hub_client_id or not sentinel_client.settings.sentinel_hub_client_secret:
        pytest.skip("Sentinel Hub credentials are not configured")

    _ = sentinel_client.request_access_token()
    raster = sentinel_client.fetch_monthly_raster(
        bbox=(32.7347, 39.8084, 32.9847, 40.0584),
        month_start=date(2024, 6, 1),
        month_end=date(2024, 6, 30),
    )
    assert raster.ndim == 3
