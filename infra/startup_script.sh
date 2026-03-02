#!/usr/bin/env bash
set -euo pipefail

metadata() {
  local key="$1"
  curl -fsS -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${key}" || true
}

apt-get update
apt-get install -y python3-pip git

REPO_URL="$(metadata REPO_URL)"
if [[ -z "${REPO_URL}" ]]; then
  REPO_URL="https://github.com/replace-me/orion-mvp.git"
fi

if [[ ! -d /opt/orion-backend/.git ]]; then
  cd /opt
  git clone "${REPO_URL}" orion-backend
else
  cd /opt/orion-backend
  git pull --ff-only
fi

pip3 install --upgrade pip
pip3 install \
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

cd /opt/orion-backend/backend
nohup python3 -m aws_era5_parallel \
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
