from __future__ import annotations

import re

from ops import bq_external_tables


class _FakeQueryJob:
    def __init__(self, rows=None, error: Exception | None = None):
        self._rows = rows or []
        self._error = error

    def result(self):
        if self._error:
            raise self._error
        return self._rows


def _extract_table_name(sql: str) -> str | None:
    match = re.search(r"`[^`]+\.[^`]+\.([A-Za-z0-9_]+)`", sql)
    return match.group(1) if match else None


class _FakeBQClient:
    def __init__(
        self,
        *,
        row_counts: dict[str, int] | None = None,
        fail_create_for: set[str] | None = None,
        fail_count_for: set[str] | None = None,
        fail_coverage_for: set[str] | None = None,
    ):
        self.row_counts = row_counts or {}
        self.fail_create_for = fail_create_for or set()
        self.fail_count_for = fail_count_for or set()
        self.fail_coverage_for = fail_coverage_for or set()
        self.queries: list[str] = []

    def query(self, sql: str, location: str | None = None):
        self.queries.append(sql)
        table = _extract_table_name(sql) or ""
        if sql.startswith("CREATE SCHEMA"):
            return _FakeQueryJob()
        if "CREATE OR REPLACE EXTERNAL TABLE" in sql:
            if table in self.fail_create_for:
                return _FakeQueryJob(error=RuntimeError(f"create failed for {table}"))
            return _FakeQueryJob()
        if "SELECT COUNT(*) AS row_count" in sql:
            if table in self.fail_count_for:
                return _FakeQueryJob(error=RuntimeError(f"count failed for {table}"))
            return _FakeQueryJob(rows=[{"row_count": self.row_counts.get(table, 0)}])
        if "year_month_count" in sql:
            if table in self.fail_coverage_for:
                return _FakeQueryJob(error=RuntimeError(f"coverage failed for {table}"))
            return _FakeQueryJob(
                rows=[{"min_year": 2020, "max_year": 2024, "year_month_count": 48}]
            )
        return _FakeQueryJob()


class _FakeStorageBucket:
    def __init__(self, location: str | None):
        self.location = location


class _FakeStorageClient:
    def __init__(
        self,
        *,
        location: str | None = "EUROPE-WEST1",
        existing_prefixes: set[str] | None = None,
        fail_get_bucket: bool = False,
    ):
        self._location = location
        self._existing_prefixes = existing_prefixes or set()
        self._fail_get_bucket = fail_get_bucket

    def get_bucket(self, bucket_name: str):
        if self._fail_get_bucket:
            raise RuntimeError(f"cannot read location for {bucket_name}")
        return _FakeStorageBucket(self._location)

    def list_blobs(self, bucket_name: str, prefix: str, max_results: int = 1):
        if prefix in self._existing_prefixes:
            return [object()]
        return []


def test_table_specs_are_exact_revised_7():
    bucket = "demo-bucket"
    specs = bq_external_tables.build_table_specs(bucket)

    assert [spec.table_name for spec in specs] == [
        "era5_daily",
        "aws_era5_daily",
        "cams_daily",
        "glofas_daily",
        "openmeteo_daily",
        "nasa_smap_daily",
        "nasa_modis_monthly",
    ]
    assert [spec.uri for spec in specs] == [
        f"gs://{bucket}/features/daily/year=*/month=*/part-*.parquet",
        f"gs://{bucket}/features/daily/year=*/month=*/part-*.parquet",
        f"gs://{bucket}/features/daily/cams/year=*/month=*/part-*.parquet",
        f"gs://{bucket}/features/daily/glofas/year=*/month=*/part-*.parquet",
        f"gs://{bucket}/features/daily/openmeteo/year=*/month=*/part-*.parquet",
        f"gs://{bucket}/features/daily/smap/year=*/month=*/part-*.parquet",
        f"gs://{bucket}/features/monthly/modis_burned/year=*/month=*/part-*.parquet",
    ]


def test_location_resolution_prefers_env_then_bucket_then_us():
    storage_client = _FakeStorageClient(location="europe-west1")

    env_location, warnings = bq_external_tables.resolve_dataset_location(
        explicit_location="eu",
        bucket_name="bucket-1",
        storage_client=storage_client,
    )
    assert env_location == "EU"
    assert warnings == []

    detected_location, warnings = bq_external_tables.resolve_dataset_location(
        explicit_location=None,
        bucket_name="bucket-1",
        storage_client=storage_client,
    )
    assert detected_location == "EUROPE-WEST1"
    assert warnings == []

    fallback_location, warnings = bq_external_tables.resolve_dataset_location(
        explicit_location=None,
        bucket_name="bucket-1",
        storage_client=_FakeStorageClient(fail_get_bucket=True),
    )
    assert fallback_location == "US"
    assert warnings


def test_apply_warns_and_skips_missing_prefix():
    specs = bq_external_tables.build_table_specs("demo-bucket")
    storage_client = _FakeStorageClient(existing_prefixes={"features/daily/"})
    bq_client = _FakeBQClient()

    report = bq_external_tables.apply_external_tables(
        project_id="demo-project",
        dataset_id="orion_features",
        location="US",
        bucket_name="demo-bucket",
        specs=specs,
        bq_client=bq_client,
        storage_client=storage_client,
    )

    assert report["query_errors"] == []
    assert "era5_daily" in report["ok_sources"]
    assert "aws_era5_daily" in report["ok_sources"]
    assert any(item["source"] == "cams_daily" for item in report["missing_prefixes"])
    assert any("No objects found" in warning for warning in report["warnings"])


def test_sanity_warns_on_empty_table():
    spec = [
        each
        for each in bq_external_tables.build_table_specs("demo-bucket")
        if each.table_name == "openmeteo_daily"
    ]
    storage_client = _FakeStorageClient(existing_prefixes={"features/daily/openmeteo/"})
    bq_client = _FakeBQClient(row_counts={"openmeteo_daily": 0})

    report = bq_external_tables.run_sanity_queries(
        project_id="demo-project",
        dataset_id="orion_features",
        location="US",
        bucket_name="demo-bucket",
        specs=spec,
        bq_client=bq_client,
        storage_client=storage_client,
    )

    assert report["query_errors"] == []
    assert report["empty_sources"] == ["openmeteo_daily"]
    assert report["row_counts"]["openmeteo_daily"] == 0


def test_sanity_collects_query_errors_and_sets_nonzero(monkeypatch):
    specs = bq_external_tables.build_table_specs("demo-bucket")
    prefixes = {spec.object_prefix for spec in specs}
    row_counts = {spec.table_name: 5 for spec in specs}
    bq_client = _FakeBQClient(row_counts=row_counts, fail_count_for={"glofas_daily"})
    storage_client = _FakeStorageClient(existing_prefixes=prefixes, location="us")

    monkeypatch.setenv("GCP_PROJECT_ID", "demo-project")
    monkeypatch.setenv("ERA5_GCS_BUCKET", "demo-bucket")
    monkeypatch.delenv("BQ_LOCATION", raising=False)
    monkeypatch.setattr(bq_external_tables, "_create_storage_client", lambda project_id: storage_client)
    monkeypatch.setattr(bq_external_tables, "_create_clients", lambda project_id: (bq_client, storage_client))

    exit_code = bq_external_tables.main(["sanity"])
    assert exit_code == 1
