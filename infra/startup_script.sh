#!/usr/bin/env bash
set -euo pipefail

metadata() {
  local key="$1"
  curl -fsS -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${key}" || true
}

apt-get update
apt-get install -y python3-pip python3-venv git

REPO_URL="$(metadata REPO_URL)"
if [[ -z "${REPO_URL}" ]]; then
  echo "REPO_URL metadata is missing; aborting startup." >&2
  exit 1
fi

if [[ ! -d /opt/orion-backend/.git ]]; then
  cd /opt
  git clone "${REPO_URL}" orion-backend
else
  cd /opt/orion-backend
  git pull --ff-only
fi

python3 -m venv /opt/orion-venv
source /opt/orion-venv/bin/activate
pip install --upgrade pip
pip install \
  s3fs \
  "fsspec[gcs]" \
  gcsfs \
  "dask[distributed]" \
  pyarrow \
  xarray \
  scipy \
  numpy \
  pandas \
  google-cloud-storage \
  sqlalchemy \
  psycopg[binary]

GCS_BUCKET="$(metadata GCS_BUCKET)"
if [[ -z "${GCS_BUCKET}" ]]; then
  GCS_BUCKET="orion-labs-mvp-era5-126886725893"
fi

BACKFILL_START="$(metadata BACKFILL_START)"
BACKFILL_END="$(metadata BACKFILL_END)"
N_WORKERS="$(metadata N_WORKERS)"
POINTS_SET="$(metadata POINTS_SET)"
if [[ -z "${BACKFILL_START}" ]]; then BACKFILL_START="1950-01-01"; fi
if [[ -z "${BACKFILL_END}" ]]; then BACKFILL_END="2026-12-31"; fi
if [[ -z "${N_WORKERS}" ]]; then N_WORKERS="14"; fi
if [[ -z "${POINTS_SET}" ]]; then POINTS_SET="provinces"; fi

export ERA5_GCS_BUCKET="${GCS_BUCKET}"
export GCS_BUCKET="${GCS_BUCKET}"
export BACKFILL_START="${BACKFILL_START}"
export BACKFILL_END="${BACKFILL_END}"
export N_WORKERS="${N_WORKERS}"
export AWS_ERA5_BUCKET="${AWS_ERA5_BUCKET:-nsf-ncar-era5}"
export AWS_ERA5_REGION="${AWS_ERA5_REGION:-us-west-2}"
export AWS_ERA5_USE_UNSIGNED="${AWS_ERA5_USE_UNSIGNED:-true}"

DATABASE_URL="$(metadata DATABASE_URL)"
if [[ -n "${DATABASE_URL}" ]]; then
  export DATABASE_URL="${DATABASE_URL}"
fi

INSTANCE_CONNECTION_NAME="$(metadata INSTANCE_CONNECTION_NAME)"
if [[ -n "${INSTANCE_CONNECTION_NAME}" ]]; then
  mkdir -p /cloudsql
  curl -fsSL -o /usr/local/bin/cloud-sql-proxy \
    "https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.13.0/cloud-sql-proxy.linux.amd64"
  chmod +x /usr/local/bin/cloud-sql-proxy
  nohup /usr/local/bin/cloud-sql-proxy \
    --unix-socket /cloudsql \
    "${INSTANCE_CONNECTION_NAME}" \
    >> /var/log/cloud-sql-proxy.log 2>&1 &
fi

cd /opt/orion-backend/backend
nohup /opt/orion-venv/bin/python -m pipeline.aws_era5_parallel \
  --start "${BACKFILL_START}" \
  --end "${BACKFILL_END}" \
  --workers "${N_WORKERS}" \
  --points-set "${POINTS_SET}" \
  --mode streaming \
  >> /var/log/orion-backfill.log 2>&1 &

if command -v gsutil >/dev/null 2>&1; then
  echo "{\"status\":\"started\",\"started_at\":\"$(date -Iseconds)\"}" | gsutil cp - "gs://${GCS_BUCKET}/backfill-status/status.json"
else
  python3 - <<PY
import json
from datetime import datetime, timezone
from google.cloud import storage

bucket_name = "${GCS_BUCKET}"
payload = {"status": "started", "started_at": datetime.now(timezone.utc).isoformat()}
client = storage.Client()
blob = client.bucket(bucket_name).blob("backfill-status/status.json")
blob.upload_from_string(json.dumps(payload), content_type="application/json")
PY
fi
