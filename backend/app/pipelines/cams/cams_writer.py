from __future__ import annotations

from datetime import date
from pathlib import Path
import tempfile
from uuid import uuid4

from google.cloud import storage
import pandas as pd

from app.config import settings


def _storage_client() -> storage.Client:
    return storage.Client()


def _bucket_name() -> str:
    if not settings.era5_gcs_bucket:
        raise RuntimeError("ERA5_GCS_BUCKET is missing")
    return settings.era5_gcs_bucket


def month_object_name(month: date) -> str:
    return f"features/daily/cams/year={month.year:04d}/month={month.month:02d}/part-0.parquet"


def month_gcs_uri(month: date) -> str:
    return f"gs://{_bucket_name()}/{month_object_name(month)}"


def month_object_exists(month: date) -> bool:
    client = _storage_client()
    blob = client.bucket(_bucket_name()).blob(month_object_name(month))
    return bool(blob.exists())


def write_month_parquet(frame: pd.DataFrame, month: date, *, overwrite: bool) -> str:
    object_name = month_object_name(month)
    client = _storage_client()
    bucket = client.bucket(_bucket_name())
    blob = bucket.blob(object_name)

    if blob.exists() and not overwrite:
        return f"gs://{_bucket_name()}/{object_name}"

    local = Path(tempfile.gettempdir()) / f"orion_cams_{month.year:04d}_{month.month:02d}_{uuid4().hex[:8]}.parquet"
    frame.to_parquet(local, index=False)
    blob.upload_from_filename(str(local))
    return f"gs://{_bucket_name()}/{object_name}"
