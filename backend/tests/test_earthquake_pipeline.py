from __future__ import annotations

from datetime import datetime, timezone, date
import json

from pipeline.earthquake_ingestion import (
    NormalizedEvent,
    build_day_partition_object_name,
    deduplicate_events,
    parse_afad_datetime,
)


def test_deduplicate_events_merges_close_usgs_and_afad_events():
    usgs = NormalizedEvent(
        raw_source="usgs",
        native_id="us7000abcd",
        event_time_utc=datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc),
        lat=38.0000,
        lon=37.0000,
        depth_km=8.0,
        magnitude=4.6,
        magnitude_type="mb",
        place="Turkey",
    )
    afad = NormalizedEvent(
        raw_source="afad",
        native_id="708164",
        event_time_utc=datetime(2026, 3, 1, 12, 0, 1, tzinfo=timezone.utc),
        lat=38.0100,
        lon=37.0100,
        depth_km=7.9,
        magnitude=4.7,
        magnitude_type=None,
        place="Test",
    )

    out = deduplicate_events([usgs, afad])

    assert len(out.index) == 1
    row = out.iloc[0]
    source_ids = json.loads(row["source_ids"])
    assert source_ids["usgs_id"] == "us7000abcd"
    assert source_ids["afad_event_id"] == "708164"
    assert row["source_primary"] == "usgs"


def test_build_day_partition_object_name():
    out = build_day_partition_object_name(date(2026, 3, 1))
    assert out == "reference/earthquakes/events/year=2026/month=03/day=01/part-0.parquet"


def test_parse_afad_datetime_naive_is_utc():
    out = parse_afad_datetime("2026-02-26T17:17:08")
    assert out.tzinfo is not None
    assert out.utcoffset() == timezone.utc.utcoffset(out)
    assert out.isoformat() == "2026-02-26T17:17:08+00:00"
