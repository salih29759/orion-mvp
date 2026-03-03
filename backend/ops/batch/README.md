# ERA5 Backfill on Cheap GCP Compute (Batch + Spot VM)

This folder contains **runner wrappers only** for heavy backfills on low-cost compute.
Core ingestion/backfill logic stays in existing code paths (`pipeline.aws_era5_parallel` and DB progress tables).

## Files

- `run_era5_backfill_batch.sh`: renders `job.json` with env vars and submits a Cloud Batch job.
- `job.json`: Cloud Batch template (Spot provisioning) for ERA5-style backfill.
- `startup.sh`: startup script for a Spot VM that runs the same backfill command in a container.

## Required Environment Variables

Set these before submitting/running:

- `DATABASE_URL`
- `ERA5_GCS_BUCKET`
- `ORION_BACKEND_API_KEY`

Additional required vars for Cloud Batch wrapper:

- `PROJECT_ID`
- `REGION`
- `BACKFILL_IMAGE` (container image that includes `/app` backend code)

## Option B (Recommended): Cloud Batch Job

1) Set runtime/config vars:

```bash
export PROJECT_ID="your-gcp-project"
export REGION="us-central1"
export BACKFILL_IMAGE="us-central1-docker.pkg.dev/${PROJECT_ID}/orion/backend:latest"
export DATABASE_URL="postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME"
export ERA5_GCS_BUCKET="orion-era5-data"
export ORION_BACKEND_API_KEY="replace-me"
export BACKFILL_START="1950-01-01"
export BACKFILL_END="2026-12-31"
export WORKERS="14"
export POINTS_SET="assets+provinces"
export PROCESSING_MODE="streaming"
export FORCE="false"
```

2) Submit with wrapper:

```bash
cd backend
chmod +x ops/batch/run_era5_backfill_batch.sh
./ops/batch/run_era5_backfill_batch.sh
```

3) Explicit `gcloud batch jobs submit` flow (render first, then submit):

```bash
JOB_NAME="era5-backfill-$(date +%Y%m%d-%H%M%S)"
DRY_RUN=true ./ops/batch/run_era5_backfill_batch.sh > /tmp/era5-backfill-job.json
gcloud batch jobs submit "${JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --config "/tmp/era5-backfill-job.json"
```

Using `./ops/batch/run_era5_backfill_batch.sh` directly is the shortest path for production.

## Option A: Spot VM Startup Script

### One-shot Spot VM example

```bash
gcloud compute instances create "era5-spot-backfill-$(date +%Y%m%d-%H%M%S)" \
  --project "${PROJECT_ID}" \
  --zone "us-central1-a" \
  --machine-type "e2-standard-8" \
  --provisioning-model=SPOT \
  --instance-termination-action=STOP \
  --maintenance-policy=TERMINATE \
  --service-account "batch-backfill@${PROJECT_ID}.iam.gserviceaccount.com" \
  --scopes "https://www.googleapis.com/auth/cloud-platform" \
  --image-family "ubuntu-2204-lts" \
  --image-project "ubuntu-os-cloud" \
  --metadata-from-file startup-script="backend/ops/batch/startup.sh" \
  --metadata "DATABASE_URL=${DATABASE_URL},ERA5_GCS_BUCKET=${ERA5_GCS_BUCKET},ORION_BACKEND_API_KEY=${ORION_BACKEND_API_KEY},BACKFILL_IMAGE=${BACKFILL_IMAGE},BACKFILL_START=${BACKFILL_START},BACKFILL_END=${BACKFILL_END},WORKERS=${WORKERS},POINTS_SET=${POINTS_SET},PROCESSING_MODE=${PROCESSING_MODE},FORCE=${FORCE},AUTO_SHUTDOWN=true"
```

### Instance template example (reusable)

```bash
gcloud compute instance-templates create era5-spot-backfill-template \
  --project "${PROJECT_ID}" \
  --machine-type "e2-standard-8" \
  --provisioning-model=SPOT \
  --instance-termination-action=STOP \
  --maintenance-policy=TERMINATE \
  --service-account "batch-backfill@${PROJECT_ID}.iam.gserviceaccount.com" \
  --scopes "https://www.googleapis.com/auth/cloud-platform" \
  --image-family "ubuntu-2204-lts" \
  --image-project "ubuntu-os-cloud" \
  --metadata-from-file startup-script="backend/ops/batch/startup.sh"
```

Then create instances from the template and pass only per-run metadata values:

```bash
gcloud compute instances create "era5-spot-backfill-$(date +%Y%m%d-%H%M%S)" \
  --project "${PROJECT_ID}" \
  --zone "us-central1-a" \
  --source-instance-template "era5-spot-backfill-template" \
  --metadata "DATABASE_URL=${DATABASE_URL},ERA5_GCS_BUCKET=${ERA5_GCS_BUCKET},ORION_BACKEND_API_KEY=${ORION_BACKEND_API_KEY},BACKFILL_IMAGE=${BACKFILL_IMAGE},BACKFILL_START=${BACKFILL_START},BACKFILL_END=${BACKFILL_END},WORKERS=${WORKERS},POINTS_SET=${POINTS_SET},PROCESSING_MODE=${PROCESSING_MODE},FORCE=${FORCE},AUTO_SHUTDOWN=true"
```

## Restart / Idempotency Safety

- Backfill progress is tracked in existing DB progress tables (`backfill_progress` and related job tables).
- Default behavior is non-force mode (`FORCE=false`), which skips months already marked successful.
- If Cloud Batch retries or a Spot VM is preempted, rerun with the same date range and `FORCE=false`; completed chunks are skipped.

## Observability

- Cloud Batch logs: Cloud Logging (`logsPolicy.destination=CLOUD_LOGGING` in `job.json`).
- Spot VM logs: serial console output + system logs (startup script output).
