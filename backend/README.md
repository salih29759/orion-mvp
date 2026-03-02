# Orion Backend (Real API Data: Open-Meteo + optional NASA FIRMS)

OpenAPI contract source of truth: `backend/openapi.yaml`

## Contract API

Base URL examples:

- Local: `http://localhost:8000`
- Cloud Run: `https://orion-api-<project>.run.app`

Auth header:

```bash
Authorization: Bearer <ORION_BACKEND_API_KEY>
```

Auth env vars:

- `ORION_BACKEND_API_KEY`: primary API key for Bearer auth
- `LOCAL_DEV_AUTH_BYPASS=true`: optional local-only bypass

Minimal contract curls:

```bash
curl -s "$BASE_URL/portfolios" -H "Authorization: Bearer $API_KEY"

curl -s "$BASE_URL/portfolios/demo-10-assets/risk-summary?start=2024-01-01&end=2024-01-31" \
  -H "Authorization: Bearer $API_KEY"

curl -s -X POST "$BASE_URL/scores/batch" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"assets":[{"asset_id":"a1","lat":41.01,"lon":28.97}],"start_date":"2024-01-01","end_date":"2024-01-03","climatology_version":"v1_test_2024_jan","include_perils":["heat","rain","wind","drought"]}'

curl -s -X POST "$BASE_URL/export/portfolio" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"portfolio_id":"demo-10-assets","start_date":"2024-01-01","end_date":"2024-01-31","format":"csv","include_drivers":true}'
```

Run tests:

```bash
pytest -q backend/tests
```

## Local setup

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Migrate + seed province metadata

```bash
alembic upgrade head
python scripts/seed_postgres.py
```

## Run API

```bash
uvicorn main:app --reload --port 8000
```

Docs: `http://localhost:8000/docs`

## Run pipeline (real-data backfill)

```bash
python -m pipeline.run_pipeline
```

This pipeline pulls **real weather data** from Open-Meteo (and wildfire hotspots from NASA FIRMS if `FIRMS_MAP_KEY` is set).

## ERA5 production ingestion jobs

Queue an async ingest job:

`POST /jobs/era5/ingest`

Example body:

```json
{
  "dataset": "era5-land",
  "variable_profile": "core",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "bbox": {"north": 42, "west": 26, "south": 36, "east": 45},
  "format": "netcdf"
}
```

Notes:
- `variable_profile`: `core` (default, 5 variables) or `full` (all ERA5-Land variables).
- You can still pass explicit `variables` to override presets.

Check job status:

`GET /jobs/{job_id}`

Backfill orchestrator:

- `POST /jobs/era5/backfill`
- `GET /jobs/era5/backfill/{backfill_id}`
- `GET /jobs/era5/variable-profiles`

Daily incremental update (scheduler):

- `POST /cron/era5/daily-update` with `x-cron-secret`

FIRMS ingestion (Path B):

- `POST /jobs/firms/ingest`
- `POST /cron/firms/daily-update` with `x-cron-secret`
- `GET /assets/{asset_id}/wildfire-features?window=24h|7d`
- `GET /notifications?portfolio_id=...`
- `POST /notifications/{notification_id}/ack`

Feature query:

- `GET /features/era5?lat=..&lon=..&start=YYYY-MM-DD&end=YYYY-MM-DD`
- `POST /features/era5/batch`

Export:

- `POST /export/portfolio`

Metrics:

- `GET /health/metrics`

Climatology build:

- `POST /climatology/build`

Batch scoring:

- `POST /scores/batch`
- `POST /scores/benchmark`

Portfolio summary:

- `GET /portfolios/{portfolio_id}/risk-summary?start=YYYY-MM-DD&end=YYYY-MM-DD`

Unified historical/current climate series:

`GET /jobs/climate/series?lat=...&lng=...&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

Response fields:

`{date,temp_mean,temp_max,precip_sum,wind_max,soil_moisture_mean,source}`

Weekly backfill helper (January preconfigured):

```bash
cd backend
./scripts/era5_backfill_weeks.sh \
  https://orion-api-126886725893.europe-west1.run.app \
  orion-dev-key-2024 \
  2024 \
  01
```

Two-phase production backfill (recommended):

```bash
cd backend
API_KEY=orion-dev-key-2024 ./scripts/era5_two_phase_backfill.sh
```

Or manually with resumable script:

```bash
cd backend
python scripts/era5_full_backfill.py --api-key "$API_KEY" --profile core --start-month 2010-01 --end-month 2026-12 --group-size 5 --max-inflight 2 --state-file era5_core_state.json
python scripts/era5_full_backfill.py --api-key "$API_KEY" --profile full --start-month 1950-01 --end-month 2009-12 --group-size 10 --max-inflight 1 --state-file era5_full_archive_state.json
```

Underwriting MVP baseline backfill (2015-2024):

```bash
curl -X POST "https://orion-api-126886725893.europe-west1.run.app/jobs/era5/backfill" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "start_month":"2015-01",
    "end_month":"2024-12",
    "bbox":{"north":42,"west":26,"south":36,"east":45},
    "variable_profile":"core",
    "mode":"monthly",
    "dataset":"era5-land",
    "concurrency":2
  }'
```

Build monthly climatology from baseline:

```bash
curl -X POST "https://orion-api-126886725893.europe-west1.run.app/climatology/build" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "climatology_version":"v1_baseline_2015_2024",
    "baseline_start":"2015-01-01",
    "baseline_end":"2024-12-31",
    "level":"month"
  }'
```

Build DOY climatology (optional higher fidelity):

```bash
curl -X POST "https://orion-api-126886725893.europe-west1.run.app/climatology/build" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "climatology_version":"v2_doy_baseline_2015_2024",
    "baseline_start":"2015-01-01",
    "baseline_end":"2024-12-31",
    "level":"doy"
  }'
```

Batch scoring (100 assets style):

```bash
curl -X POST "https://orion-api-126886725893.europe-west1.run.app/scores/batch" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "assets":[{"id":"ist-1","lat":41.01,"lon":28.97},{"id":"ank-1","lat":39.93,"lon":32.85}],
    "start_date":"2024-01-01",
    "end_date":"2024-03-31",
    "climatology_version":"v1_baseline_2015_2024",
    "persist": true
  }'
```

Benchmark (100 assets x 90 days):

```bash
curl -X POST "https://orion-api-126886725893.europe-west1.run.app/scores/benchmark" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "assets_count":100,
    "start_date":"2024-01-01",
    "end_date":"2024-03-30",
    "climatology_version":"v1_baseline_2015_2024"
  }'
```

FIRMS ingest example:

```bash
curl -X POST "https://orion-api-126886725893.europe-west1.run.app/jobs/firms/ingest" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "source":"VIIRS_SNPP_NRT",
    "bbox":{"north":42,"west":26,"south":36,"east":45},
    "start_date":"2026-02-28",
    "end_date":"2026-03-01"
  }'
```

Daily FIRMS cron trigger:

```bash
curl -X POST "https://orion-api-126886725893.europe-west1.run.app/cron/firms/daily-update" \
  -H "x-cron-secret: $CRON_SECRET"
```

## CDS / ERA5 smoke test

After adding `CDSAPI_KEY`, run:

```bash
python scripts/test_cdsapi.py
```

Or via API (scheduler secret required):

`POST /internal/cds/test`

## Scheduler trigger endpoint

For automatic daily refresh, call:

`POST /internal/pipeline/run`

Required header:

`x-cron-secret: <CRON_SECRET>`

Optional query:

`backfill_days=2`

Check latest run:

`GET /internal/pipeline/status` with same `x-cron-secret` header.

## Required env vars in production

- `DATABASE_URL`
- `API_KEY`
- `ALLOWED_ORIGINS`
- `MODEL_VERSION`
- `DEFAULT_DATA_SOURCE`
- `FIRMS_MAP_KEY` (optional, for wildfire alerts)
- `FIRMS_MAP_KEY` is used with NASA FIRMS Area CSV endpoint:
  - `https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{W,S,E,N}/{DAY_RANGE}`
- `FIRMS_SOURCE` (optional, default `VIIRS_SNPP_NRT`)
- `FIRMS_DAY_RANGE` (optional, default `2`)
- `WILDFIRE_RADIUS_KM` (optional, default `75`)
- `CRON_SECRET` (required for scheduler endpoint)
- `DAILY_BACKFILL_DAYS` (optional default for scheduler runs, default `2`)
- `CDSAPI_URL` (optional, default `https://cds.climate.copernicus.eu/api`)
- `CDSAPI_KEY` (optional, required for CDS smoke test)
- `CDS_DATASET` (optional, default `reanalysis-era5-single-levels`; jobs can override with `era5-land`)
- `CDS_VARIABLE` (optional, default `2m_temperature`)
- `CDS_AREA_NORTH/WEST/SOUTH/EAST` (optional bounding box for test request)
- `ERA5_GCS_BUCKET` (required for ERA5 ingest jobs)
- `ERA5_MAX_CONCURRENT_JOBS` (optional, default `1`)

## Secret Manager setup (recommended)

Do not keep CDS credentials in plain env vars.

```bash
echo -n 'YOUR_CDS_API_KEY' | gcloud secrets create cdsapi-key --data-file=- --project orion-labs-mvp
# if secret exists, add a new version instead:
# echo -n 'YOUR_CDS_API_KEY' | gcloud secrets versions add cdsapi-key --data-file=- --project orion-labs-mvp

PROJECT_NUMBER=$(gcloud projects describe orion-labs-mvp --format='value(projectNumber)')
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
gcloud secrets add-iam-policy-binding cdsapi-key \
  --project orion-labs-mvp \
  --member="serviceAccount:${SA}" \
  --role="roles/secretmanager.secretAccessor"

gcloud run services update orion-api \
  --region europe-west1 \
  --project orion-labs-mvp \
  --set-secrets CDSAPI_KEY=cdsapi-key:latest
```

## Edge-case runbook

- `ERA5 job concurrency limit reached`:
  - Wait for running jobs to complete, then re-submit remaining chunks.
- Job stuck in `running` for a long time:
  - recover stale jobs:
    - `POST /internal/jobs/recover?stale_minutes=5` with `x-cron-secret`.
- CDS intermittent failures:
  - Retriever already retries with exponential backoff.
  - Re-submit same payload: request signature deduplicates successful/running jobs.
- Monthly requests timing out/stalling:
  - Use weekly chunks (script above) for reliable ingestion.
