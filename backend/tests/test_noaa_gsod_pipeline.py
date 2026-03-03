from __future__ import annotations

from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.orm import ProvinceORM
from pipeline import noaa_gsod


class _FakeBlob:
    def __init__(self, store: dict[str, bytes], name: str):
        self._store = store
        self._name = name

    def exists(self) -> bool:
        return self._name in self._store

    def upload_from_filename(self, filename: str) -> None:
        self._store[self._name] = Path(filename).read_bytes()

    def upload_from_string(self, data: str, content_type: str | None = None) -> None:
        _ = content_type
        self._store[self._name] = data.encode("utf-8")


class _FakeBucket:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def blob(self, name: str) -> _FakeBlob:
        return _FakeBlob(self._store, name)


class _FakeStorageClient:
    def __init__(self, store: dict[str, bytes]):
        self._store = store

    def bucket(self, bucket_name: str) -> _FakeBucket:
        _ = bucket_name
        return _FakeBucket(self._store)


class _FakeResponse:
    def __init__(self, *, status_code: int, text: str = "", json_payload=None):
        self.status_code = status_code
        self.text = text
        self._json_payload = json_payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self._json_payload


def test_noaa_station_list_month_process_and_fallback(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)

    monkeypatch.setattr(noaa_gsod, "SessionLocal", TestSession)
    monkeypatch.setattr(noaa_gsod.settings, "era5_gcs_bucket", "test-bucket")

    with TestSession() as db:
        db.add(
            ProvinceORM(
                id="34",
                plate=34,
                name="Istanbul",
                region="Marmara",
                lat=41.0,
                lng=29.0,
                population=15000000,
                insured_assets=270000000000,
            )
        )
        db.commit()

    store: dict[str, bytes] = {}
    monkeypatch.setattr(noaa_gsod, "_storage_client", lambda: _FakeStorageClient(store))

    calls = {"primary": 0, "fallback": 0, "station_list": 0}

    station_list_csv = "\n".join(
        [
            '"USAF","WBAN","STATION NAME","CTRY","STATE","ICAO","LAT","LON","ELEV(M)","BEGIN","END"',
            '"170200","99999","BARTIN","TU","","","+41.633","+032.333","+0033.0","19510101","20250824"',
            '"010010","99999","JAN MAYEN","NO","","ENJA","+70.933","-008.667","+0009.0","19310101","20250824"',
        ]
    )

    fallback_json = [
        {
            "STATION": "17020099999",
            "DATE": "2024-01-02",
            "TEMP": "104.0",
            "MAX": "104.0",
            "MIN": "32.0",
            "PRCP": "2.00",
            "WDSP": "30.0",
            "GUST": "999.9",
        }
    ]

    def fake_get(url, params=None, timeout=60):
        _ = timeout
        if url == noaa_gsod.ISD_HISTORY_URL:
            calls["station_list"] += 1
            return _FakeResponse(status_code=200, text=station_list_csv)

        primary_url = noaa_gsod.GSOD_PRIMARY_TEMPLATE.format(year=2024, station_id="17020099999")
        if url == primary_url:
            calls["primary"] += 1
            return _FakeResponse(status_code=404, text="not found")

        if url == noaa_gsod.GSOD_FALLBACK_URL:
            calls["fallback"] += 1
            assert params is not None
            assert params["stations"] == "17020099999"
            return _FakeResponse(status_code=200, json_payload=fallback_json)

        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(noaa_gsod.requests, "get", fake_get)

    stations = noaa_gsod.download_turkey_station_list()
    assert len(stations.index) > 0

    run_id = "noaa_test_run"
    month = date(2024, 1, 1)
    now = datetime.now(timezone.utc)
    noaa_gsod._insert_pending_rows(run_id=run_id, months=[month], now=now)
    noaa_gsod._run_backfill(
        run_id=run_id,
        months=[month],
        requested_start=date(2024, 1, 1),
        requested_end=date(2024, 1, 31),
        concurrency=1,
        force=False,
    )

    feature_key = noaa_gsod.month_object_name(month)
    metadata_key = noaa_gsod.metadata_object_name()

    assert feature_key in store
    assert metadata_key in store

    frame = pd.read_parquet(BytesIO(store[feature_key]))
    assert list(frame.columns) == noaa_gsod.FEATURE_COLUMNS
    assert len(frame.index) == 1

    row = frame.iloc[0]
    assert str(row["station_id"]) == "17020099999"
    assert str(row["province_id"]) == "34"
    assert abs(float(row["temp_mean_c"]) - 40.0) < 1e-6
    assert abs(float(row["precip_mm"]) - 50.8) < 1e-6
    assert row["wind_gust_ms"] is None or pd.isna(row["wind_gust_ms"])
    assert bool(row["heat_extreme"]) is True
    assert bool(row["frost_event"]) is True
    assert bool(row["heavy_rain"]) is True
    assert bool(row["strong_wind"]) is True

    status = noaa_gsod.get_latest_status()
    assert status["status"] == "completed"
    assert status["strong_wind_proxy_used"] == 1

    assert calls["station_list"] >= 1
    assert calls["primary"] >= 1
    assert calls["fallback"] >= 1
