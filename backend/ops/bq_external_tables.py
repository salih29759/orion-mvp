#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any

try:
    import google.auth  # type: ignore[import]
except Exception:  # pragma: no cover - optional dependency at import-time
    google = None  # type: ignore[assignment]

try:
    from google.cloud import bigquery  # type: ignore[import]
except Exception:  # pragma: no cover - optional dependency at import-time
    bigquery = None  # type: ignore[assignment]

try:
    from google.cloud import storage  # type: ignore[import]
except Exception:  # pragma: no cover - optional dependency at import-time
    storage = None  # type: ignore[assignment]


DATASET_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")


@dataclass(frozen=True)
class ExternalTableSpec:
    table_name: str
    uri: str
    hive_partition_uri_prefix: str
    object_prefix: str


TABLE_LAYOUTS: tuple[tuple[str, str, str], ...] = (
    ("era5_daily", "features/daily/year=*/month=*/part-*.parquet", "features/daily"),
    ("aws_era5_daily", "features/daily/year=*/month=*/part-*.parquet", "features/daily"),
    ("cams_daily", "features/daily/cams/year=*/month=*/part-*.parquet", "features/daily/cams"),
    ("glofas_daily", "features/daily/glofas/year=*/month=*/part-*.parquet", "features/daily/glofas"),
    ("openmeteo_daily", "features/daily/openmeteo/year=*/month=*/part-*.parquet", "features/daily/openmeteo"),
    ("nasa_smap_daily", "features/daily/smap/year=*/month=*/part-*.parquet", "features/daily/smap"),
    ("nasa_modis_monthly", "features/monthly/modis_burned/year=*/month=*/part-*.parquet", "features/monthly/modis_burned"),
)


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value and value.strip():
            return value.strip()
    return None


def _project_from_adc() -> str | None:
    if "google" not in globals() or google is None:  # type: ignore[name-defined]
        return None
    try:
        _, project_id = google.auth.default()  # type: ignore[attr-defined]
    except Exception:
        return None
    return project_id


def resolve_project_id(project_id_arg: str | None) -> str:
    project_id = _first_non_empty(project_id_arg, os.getenv("GCP_PROJECT_ID"), os.getenv("BQ_PROJECT_ID"), _project_from_adc())
    if not project_id:
        raise ValueError("Missing project id. Set GCP_PROJECT_ID (or BQ_PROJECT_ID) or pass --project-id.")
    return project_id


def resolve_dataset_id(dataset_id_arg: str | None) -> str:
    dataset_id = _first_non_empty(dataset_id_arg, os.getenv("BQ_DATASET_ID"), os.getenv("BQ_DATASET"), "orion_features")
    if not dataset_id:
        raise ValueError("Missing dataset id.")
    if not DATASET_ID_RE.fullmatch(dataset_id):
        raise ValueError(f"Invalid dataset id: {dataset_id!r}")
    return dataset_id


def resolve_bucket(bucket_arg: str | None) -> str:
    bucket = _first_non_empty(bucket_arg, os.getenv("ERA5_GCS_BUCKET"))
    if not bucket:
        raise ValueError("Missing ERA5_GCS_BUCKET (or --bucket).")
    return bucket


def resolve_dataset_location(
    explicit_location: str | None,
    bucket_name: str,
    storage_client: Any | None,
) -> tuple[str, list[str]]:
    warnings: list[str] = []
    if explicit_location:
        return explicit_location.strip().upper(), warnings

    if storage_client is not None:
        try:
            bucket = storage_client.get_bucket(bucket_name)
            location = (getattr(bucket, "location", None) or "").strip()
            if location:
                return location.upper(), warnings
            warnings.append(
                f"Bucket {bucket_name} location is empty; defaulting dataset location to US."
            )
        except Exception as exc:  # noqa: BLE001
            warnings.append(
                f"Could not detect bucket location for {bucket_name}: {exc}. Defaulting dataset location to US."
            )
    else:
        warnings.append("Storage client unavailable; defaulting dataset location to US.")

    return "US", warnings


def build_table_specs(bucket_name: str) -> list[ExternalTableSpec]:
    specs: list[ExternalTableSpec] = []
    for table_name, uri_suffix, root_suffix in TABLE_LAYOUTS:
        specs.append(
            ExternalTableSpec(
                table_name=table_name,
                uri=f"gs://{bucket_name}/{uri_suffix}",
                hive_partition_uri_prefix=f"gs://{bucket_name}/{root_suffix}",
                object_prefix=f"{root_suffix.rstrip('/')}/",
            )
        )
    return specs


def duplicate_uri_warnings(specs: list[ExternalTableSpec]) -> list[str]:
    seen: dict[str, str] = {}
    warnings: list[str] = []
    for spec in specs:
        previous = seen.get(spec.uri)
        if previous:
            warnings.append(
                f"Tables {previous} and {spec.table_name} share the same external URI: {spec.uri}"
            )
        else:
            seen[spec.uri] = spec.table_name
    return warnings


def _sql_literal(value: str) -> str:
    return value.replace("'", "\\'")


def _table_fqn(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"`{project_id}.{dataset_id}.{table_name}`"


def build_create_schema_sql(project_id: str, dataset_id: str, location: str) -> str:
    return (
        f"CREATE SCHEMA IF NOT EXISTS `{project_id}.{dataset_id}` "
        f"OPTIONS(location='{_sql_literal(location)}')"
    )


def build_create_external_table_sql(project_id: str, dataset_id: str, spec: ExternalTableSpec) -> str:
    return f"""
CREATE OR REPLACE EXTERNAL TABLE {_table_fqn(project_id, dataset_id, spec.table_name)}
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['{_sql_literal(spec.uri)}'],
  hive_partition_uri_prefix = '{_sql_literal(spec.hive_partition_uri_prefix)}',
  require_hive_partition_filter = FALSE
)
""".strip()


def build_row_count_sql(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"SELECT COUNT(*) AS row_count FROM {_table_fqn(project_id, dataset_id, table_name)}"


def build_partition_coverage_sql(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"""
SELECT
  MIN(SAFE_CAST(year AS INT64)) AS min_year,
  MAX(SAFE_CAST(year AS INT64)) AS max_year,
  COUNT(DISTINCT FORMAT('%04d-%02d', SAFE_CAST(year AS INT64), SAFE_CAST(month AS INT64))) AS year_month_count
FROM {_table_fqn(project_id, dataset_id, table_name)}
WHERE SAFE_CAST(year AS INT64) IS NOT NULL
  AND SAFE_CAST(month AS INT64) IS NOT NULL
""".strip()


def _run_query(client: Any, sql: str, *, location: str) -> list[Any]:
    try:
        query_job = client.query(sql, location=location)
    except TypeError:
        query_job = client.query(sql)
    rows = query_job.result()
    return list(rows)


def _row_get(row: Any, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    if hasattr(row, field):
        return getattr(row, field)
    try:
        return row[field]
    except Exception:  # noqa: BLE001
        return None


def prefix_has_objects(storage_client: Any, bucket_name: str, object_prefix: str) -> bool:
    blobs = storage_client.list_blobs(bucket_name, prefix=object_prefix, max_results=1)
    for _ in blobs:
        return True
    return False


def _empty_report(specs: list[ExternalTableSpec]) -> dict[str, Any]:
    return {
        "checked_sources_total": len(specs),
        "ok_sources": [],
        "empty_sources": [],
        "missing_prefixes": [],
        "query_errors": [],
        "row_counts": {},
        "partition_coverage": {},
        "warnings": [],
    }


def apply_external_tables(
    *,
    project_id: str,
    dataset_id: str,
    location: str,
    bucket_name: str,
    specs: list[ExternalTableSpec],
    bq_client: Any,
    storage_client: Any,
) -> dict[str, Any]:
    report = _empty_report(specs)
    report["warnings"].extend(duplicate_uri_warnings(specs))

    try:
        _run_query(
            bq_client,
            build_create_schema_sql(project_id=project_id, dataset_id=dataset_id, location=location),
            location=location,
        )
    except Exception as exc:  # noqa: BLE001
        report["query_errors"].append({"source": "dataset", "error": str(exc)})
        return report

    for spec in specs:
        try:
            has_objects = prefix_has_objects(storage_client, bucket_name, spec.object_prefix)
        except Exception as exc:  # noqa: BLE001
            report["warnings"].append(
                f"Failed listing gs://{bucket_name}/{spec.object_prefix} for {spec.table_name}: {exc}"
            )
            report["missing_prefixes"].append(
                {"source": spec.table_name, "prefix": f"gs://{bucket_name}/{spec.object_prefix}"}
            )
            continue

        if not has_objects:
            report["warnings"].append(
                f"No objects found under gs://{bucket_name}/{spec.object_prefix}; skipping {spec.table_name}."
            )
            report["missing_prefixes"].append(
                {"source": spec.table_name, "prefix": f"gs://{bucket_name}/{spec.object_prefix}"}
            )
            continue

        try:
            _run_query(
                bq_client,
                build_create_external_table_sql(project_id=project_id, dataset_id=dataset_id, spec=spec),
                location=location,
            )
            report["ok_sources"].append(spec.table_name)
        except Exception as exc:  # noqa: BLE001
            report["query_errors"].append({"source": spec.table_name, "error": str(exc)})

    return report


def run_sanity_queries(
    *,
    project_id: str,
    dataset_id: str,
    location: str,
    bucket_name: str,
    specs: list[ExternalTableSpec],
    bq_client: Any,
    storage_client: Any,
) -> dict[str, Any]:
    report = _empty_report(specs)

    for spec in specs:
        try:
            has_objects = prefix_has_objects(storage_client, bucket_name, spec.object_prefix)
        except Exception as exc:  # noqa: BLE001
            report["warnings"].append(
                f"Failed listing gs://{bucket_name}/{spec.object_prefix} for {spec.table_name}: {exc}"
            )
            report["missing_prefixes"].append(
                {"source": spec.table_name, "prefix": f"gs://{bucket_name}/{spec.object_prefix}"}
            )
            continue

        if not has_objects:
            report["warnings"].append(
                f"No objects found under gs://{bucket_name}/{spec.object_prefix}; skipping sanity for {spec.table_name}."
            )
            report["missing_prefixes"].append(
                {"source": spec.table_name, "prefix": f"gs://{bucket_name}/{spec.object_prefix}"}
            )
            continue

        try:
            count_rows = _run_query(
                bq_client,
                build_row_count_sql(project_id=project_id, dataset_id=dataset_id, table_name=spec.table_name),
                location=location,
            )
            count = int(_row_get(count_rows[0], "row_count") if count_rows else 0)
        except Exception as exc:  # noqa: BLE001
            report["query_errors"].append({"source": spec.table_name, "error": str(exc)})
            continue

        report["row_counts"][spec.table_name] = count
        if count == 0:
            report["empty_sources"].append(spec.table_name)
            report["warnings"].append(f"{spec.table_name} has zero rows.")
            continue

        try:
            coverage_rows = _run_query(
                bq_client,
                build_partition_coverage_sql(project_id=project_id, dataset_id=dataset_id, table_name=spec.table_name),
                location=location,
            )
            coverage_row = coverage_rows[0] if coverage_rows else {}
            report["partition_coverage"][spec.table_name] = {
                "min_year": _row_get(coverage_row, "min_year"),
                "max_year": _row_get(coverage_row, "max_year"),
                "year_month_count": _row_get(coverage_row, "year_month_count"),
            }
            report["ok_sources"].append(spec.table_name)
        except Exception as exc:  # noqa: BLE001
            report["query_errors"].append({"source": spec.table_name, "error": str(exc)})

    return report


def _create_storage_client(project_id: str) -> Any | None:
    if storage is None:
        return None
    try:
        return storage.Client(project=project_id)
    except Exception:
        return None


def _create_clients(project_id: str) -> tuple[Any, Any]:
    missing: list[str] = []
    if bigquery is None:
        missing.append("google-cloud-bigquery")
    if storage is None:
        missing.append("google-cloud-storage")
    if missing:
        raise RuntimeError(f"Missing dependencies: {', '.join(missing)}")
    return bigquery.Client(project=project_id), storage.Client(project=project_id)


def _print_json(data: dict[str, Any]) -> None:
    print(json.dumps(data, indent=2, sort_keys=True))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage BigQuery external feature index tables.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command_name in ("plan", "apply", "sanity"):
        sub = subparsers.add_parser(command_name)
        sub.add_argument("--project-id", default=None, help="GCP project id (default: GCP_PROJECT_ID/BQ_PROJECT_ID/ADC)")
        sub.add_argument("--dataset-id", default=None, help="BigQuery dataset id (default: BQ_DATASET_ID/BQ_DATASET/orion_features)")
        sub.add_argument("--bucket", default=None, help="GCS bucket name (default: ERA5_GCS_BUCKET)")
        sub.add_argument("--location", default=None, help="BigQuery location override (default: BQ_LOCATION or bucket location)")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        project_id = resolve_project_id(args.project_id)
        dataset_id = resolve_dataset_id(args.dataset_id)
        bucket_name = resolve_bucket(args.bucket)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    storage_for_location = _create_storage_client(project_id)
    explicit_location = _first_non_empty(args.location, os.getenv("BQ_LOCATION"))
    location, location_warnings = resolve_dataset_location(
        explicit_location=explicit_location,
        bucket_name=bucket_name,
        storage_client=storage_for_location,
    )
    specs = build_table_specs(bucket_name)

    if args.command == "plan":
        _print_json(
            {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "location": location,
                "checked_sources_total": len(specs),
                "tables": [
                    {
                        "table_name": spec.table_name,
                        "uri": spec.uri,
                        "hive_partition_uri_prefix": spec.hive_partition_uri_prefix,
                    }
                    for spec in specs
                ],
                "warnings": location_warnings + duplicate_uri_warnings(specs),
            }
        )
        return 0

    try:
        bq_client, storage_client = _create_clients(project_id)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.command == "apply":
        report = apply_external_tables(
            project_id=project_id,
            dataset_id=dataset_id,
            location=location,
            bucket_name=bucket_name,
            specs=specs,
            bq_client=bq_client,
            storage_client=storage_client,
        )
    else:
        report = run_sanity_queries(
            project_id=project_id,
            dataset_id=dataset_id,
            location=location,
            bucket_name=bucket_name,
            specs=specs,
            bq_client=bq_client,
            storage_client=storage_client,
        )

    report["project_id"] = project_id
    report["dataset_id"] = dataset_id
    report["location"] = location
    report["warnings"] = location_warnings + report.get("warnings", [])
    _print_json(report)
    return 1 if report.get("query_errors") else 0


if __name__ == "__main__":
    sys.exit(main())
