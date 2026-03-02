from __future__ import annotations

from datetime import date

from app.services.job_service import (
    create_aws_backfill_job as _create_aws_backfill_job,
    create_aws_catalog_sync_job as _create_aws_catalog_sync_job,
    get_aws_catalog_latest as _get_aws_catalog_latest,
    get_aws_latest_status as _get_aws_latest_status,
    run_aws_monthly_update as _run_aws_monthly_update,
)


def create_catalog_sync(*, prefixes: list[str] | None, max_keys_per_prefix: int) -> dict:
    return _create_aws_catalog_sync_job(prefixes=prefixes, max_keys_per_prefix=max_keys_per_prefix)


def get_catalog_latest() -> dict:
    return _get_aws_catalog_latest()


def create_backfill(
    *,
    start: date,
    end: date,
    mode: str,
    extraction_mode: str,
    points_set: str | None,
    bbox: dict,
    variables: list[str],
    concurrency: int,
    n_workers: int,
    force: bool,
) -> dict:
    return _create_aws_backfill_job(
        start=start,
        end=end,
        mode=mode,
        extraction_mode=extraction_mode,
        points_set=points_set,
        bbox=bbox,
        variables=variables,
        concurrency=concurrency,
        n_workers=n_workers,
        force=force,
    )


def run_monthly_update(*, bbox: dict, variables: list[str], concurrency: int = 2) -> dict:
    return _run_aws_monthly_update(bbox=bbox, variables=variables, concurrency=concurrency)


def get_latest_status() -> dict:
    return _get_aws_latest_status()
