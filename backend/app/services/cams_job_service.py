from __future__ import annotations

from datetime import date

from app.pipelines.cams.cams_job import create_backfill as _create_backfill
from app.pipelines.cams.cams_job import get_status as _get_status


def create_backfill(*, start: date, end: date, concurrency: int, force: bool) -> dict:
    return _create_backfill(start=start, end=end, concurrency=concurrency, force=force)


def get_latest_status(run_id: str | None = None) -> dict:
    return _get_status(run_id=run_id)
