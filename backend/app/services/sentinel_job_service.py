from __future__ import annotations

from datetime import date

from app.pipelines.sentinel_hub.sentinel_job import create_backfill_run, get_latest_status, run_previous_month


def create_backfill(*, start: date, end: date, concurrency: int, force: bool) -> dict:
    return create_backfill_run(start=start, end=end, concurrency=concurrency, force=force)


def get_status() -> dict:
    return get_latest_status()


def run_monthly_update() -> dict:
    return run_previous_month()
