# Ops Tools

These scripts are optional runtime observability helpers for AWS ERA5 backfill.
They do not start or stop the backfill workers.

## `progress_updater.py`

Purpose: keep `gs://$ERA5_GCS_BUCKET/backfill-status/progress.json` fresh from DB state.

Run:

```bash
cd backend
export DATABASE_URL='postgresql+psycopg://...'
export ERA5_GCS_BUCKET='your-bucket'
export BACKFILL_START='2015-01-01'   # optional
export BACKFILL_END='2024-12-31'     # optional
python -m ops.progress_updater
```

Optional env:

- `PROGRESS_UPDATE_INTERVAL_SEC` (default `60`)
- `PROGRESS_UPDATE_MAX_LOOPS` (default `1440`)

## `checkpoint_monitor.py`

Purpose: write periodic checkpoint lines (JSONL) with success/failed/running, latest successful month, progress timestamp, and latest written GCS object.

Run:

```bash
cd backend
export DATABASE_URL='postgresql+psycopg://...'
export ERA5_GCS_BUCKET='your-bucket'
export BACKFILL_START='2015-01-01'   # optional
export BACKFILL_END='2024-12-31'     # optional
python -m ops.checkpoint_monitor
```

Optional env:

- `CHECKPOINT_INTERVAL_SEC` (default `1800`)
- `CHECKPOINT_MAX_LOOPS` (default `48`)
- `CHECKPOINT_LOG_PATH` (default `backfill-checkpoints.log`)

## VM Process Check

```bash
pgrep -af 'progress_updater.py|run_backfill_threads.py|cloud-sql-proxy'
```

## Note

These tools are optional and do not affect backfill execution logic.

