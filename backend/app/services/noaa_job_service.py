from __future__ import annotations

from datetime import date

from pipeline.noaa_gsod import create_backfill_run, get_latest_status, run_daily_update


def create_backfill(*, start: date, end: date, concurrency: int, force: bool) -> dict:
    return create_backfill_run(start=start, end=end, concurrency=concurrency, force=force)


def get_status() -> dict:
    return get_latest_status()


def run_daily() -> dict:
    return run_daily_update()
