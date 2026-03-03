from __future__ import annotations

from pathlib import Path
import tempfile

from google.cloud import storage
import pandas as pd


def _storage_client() -> storage.Client:
    return storage.Client()


def month_object_name(*, year: int, month: int) -> str:
    return f"features/monthly/sentinel_hub/year={year:04d}/month={month:02d}/part-0.parquet"


def month_exists(*, bucket_name: str, year: int, month: int) -> bool:
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(month_object_name(year=year, month=month)).exists()


def write_month_parquet(*, bucket_name: str, year: int, month: int, frame: pd.DataFrame) -> str:
    object_name = month_object_name(year=year, month=month)
    local = Path(tempfile.gettempdir()) / f"orion_sentinel_{year:04d}_{month:02d}.parquet"
    frame.to_parquet(local, index=False)

    client = _storage_client()
    bucket = client.bucket(bucket_name)
    bucket.blob(object_name).upload_from_filename(str(local))
    return f"gs://{bucket_name}/{object_name}"
