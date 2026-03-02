from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

from google.cloud import storage
import pandas as pd
import s3fs

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.aws_era5_stream import aggregate_daily_features, extract_points_hourly, open_era5_from_s3


def _pick_sample_key(month: str = "202001") -> str:
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": "us-west-2"})
    prefix = f"nsf-ncar-era5/e5.oper.an.sfc/{month}"
    keys = fs.ls(prefix)
    if not keys:
        raise RuntimeError(f"No keys found under {prefix}")
    t2m_keys = [k for k in keys if "_2t." in k]
    selected = t2m_keys[0] if t2m_keys else keys[0]
    return selected.replace("nsf-ncar-era5/", "", 1)


def _list_five_keys() -> list[str]:
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": "us-west-2"})
    keys = fs.ls("nsf-ncar-era5/e5.oper.an.sfc/202001")
    return [k.replace("nsf-ncar-era5/", "", 1) for k in keys[:5]]


def _write_to_gcs(df: pd.DataFrame, bucket: str, object_name: str) -> str:
    client = storage.Client()
    local_path = f"/tmp/streaming_test_{uuid4().hex}.parquet"
    df.to_parquet(local_path, index=False)
    blob = client.bucket(bucket).blob(object_name)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket}/{object_name}"


def main() -> None:
    started = time.time()
    bucket = os.getenv("ERA5_GCS_BUCKET") or os.getenv("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("ERA5_GCS_BUCKET or GCS_BUCKET must be set")

    print("1) Listing 5 sample keys...")
    keys = _list_five_keys()
    for key in keys:
        print(f" - {key}")

    print("2) Streaming one month (2020-01) for t2m...")
    s3_key = _pick_sample_key("202001")
    print(f"   selected key: {s3_key}")
    ds = open_era5_from_s3(s3_key)
    print(f"   dataset vars: {list(ds.data_vars.keys())[:5]}")

    points = [
        {"point_id": "province:34", "lat": 41.0082, "lon": 28.9784},  # Istanbul
        {"point_id": "province:06", "lat": 39.9334, "lon": 32.8597},  # Ankara
        {"point_id": "province:35", "lat": 38.4237, "lon": 27.1428},  # Izmir
    ]
    print("3) Extracting 3 province centroids...")
    hourly = extract_points_hourly(ds, points, variable_name="2m_temperature")
    ds.close()
    if hourly.empty:
        raise RuntimeError("Hourly extraction produced no rows")

    print("4) Aggregating to daily features...")
    wide = hourly.pivot_table(index=["time", "point_id", "lat", "lng"], columns="variable", values="value", aggfunc="first").reset_index()
    wide.columns.name = None
    daily = aggregate_daily_features(wide)
    if daily.empty:
        raise RuntimeError("Daily aggregation produced no rows")

    daily = daily.copy()
    daily["source"] = "aws_nsf_ncar_era5"
    daily["run_id"] = f"stream_smoke_{uuid4().hex[:8]}"
    daily["ingested_at"] = datetime.now(timezone.utc)

    print("5) Writing parquet to GCS test path...")
    object_name = f"features/daily_test/year=2020/month=01/part-smoke-{uuid4().hex[:8]}.parquet"
    out_uri = _write_to_gcs(daily, bucket, object_name)

    elapsed = time.time() - started
    print("6) Results:")
    print(f"   row_count={len(daily)}")
    print("   expected_rows=93 (31 days * 3 points)")
    print(f"   output_uri={out_uri}")
    print(f"   elapsed_seconds={elapsed:.2f}")
    print("   sample:")
    print(daily.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
