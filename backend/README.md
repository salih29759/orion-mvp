# Orion Backend (Real API Data: Open-Meteo + optional NASA FIRMS)

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
  "variables": [
    "2m_temperature",
    "total_precipitation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "volumetric_soil_water_layer_1"
  ],
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "bbox": {"north": 42, "west": 26, "south": 36, "east": 45},
  "format": "netcdf"
}
```

Check job status:

`GET /jobs/{job_id}`

Backfill orchestrator:

- `POST /jobs/era5/backfill`
- `GET /jobs/era5/backfill/{backfill_id}`

Daily incremental update (scheduler):

- `POST /cron/era5/daily-update` with `x-cron-secret`

Feature query:

- `GET /features/era5?lat=..&lon=..&start=YYYY-MM-DD&end=YYYY-MM-DD`
- `POST /features/era5/batch`

Export:

- `POST /export/portfolio`

Metrics:

- `GET /health/metrics` (requires `x-cron-secret`)

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
